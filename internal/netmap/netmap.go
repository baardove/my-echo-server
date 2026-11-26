package netmap

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/baardove/my-echo/server/internal/config"
)

const ssdpAddr = "239.255.255.250:1900"

type Mapping struct {
	Service      string `json:"service"`
	Protocol     string `json:"protocol"` // "TCP"
	InternalPort int    `json:"internalPort"`
	ExternalPort int    `json:"externalPort"`
	ExternalURL  string `json:"externalURL,omitempty"`
	Active       bool   `json:"active"`
	Error        string `json:"error,omitempty"`
}

type Status struct {
	Enabled      bool      `json:"enabled"`
	Method       string    `json:"method,omitempty"` // "upnp-igd" | "nat-pmp" | ""
	GatewayName  string    `json:"gatewayName,omitempty"`
	ExternalIP   string    `json:"externalIP,omitempty"`
	Mappings     []Mapping `json:"mappings"`
	LastError    string    `json:"lastError,omitempty"`
	LastRenewAt  string    `json:"lastRenewAt,omitempty"`
}

type igdGateway struct {
	name        string
	controlURL  string
	serviceType string
	localIP     string
}

// Manager automatically opens & renews router port mappings via UPnP-IGD or NAT-PMP
// when explicitly enabled by the user, and cleans up mappings on disable or shutdown.
type Manager struct {
	cfg        *config.Config
	httpClient *http.Client

	mu       sync.RWMutex
	status   Status
	igd      *igdGateway
	natPMPGW net.IP
	wake     chan struct{}
	once     sync.Once
}

func NewManager(cfg *config.Config) *Manager {
	return &Manager{
		cfg:        cfg,
		httpClient: &http.Client{Timeout: 6 * time.Second},
		wake:       make(chan struct{}, 4),
		status:     Status{Mappings: []Mapping{}},
	}
}

func (m *Manager) Status() Status {
	m.mu.RLock()
	defer m.mu.RUnlock()
	cp := m.status
	cp.Enabled = m.cfg.Remote.UPnPEnabled
	cp.Mappings = append([]Mapping(nil), m.status.Mappings...)
	return cp
}

func (m *Manager) TriggerSync() {
	select {
	case m.wake <- struct{}{}:
	default:
	}
}

func (m *Manager) Start(ctx context.Context) {
	m.once.Do(func() {
		go m.loop(ctx)
	})
}

func (m *Manager) loop(ctx context.Context) {
	// Sync once on startup if enabled
	if m.cfg.Remote.UPnPEnabled {
		m.Sync(ctx)
	}
	ticker := time.NewTicker(25 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			// Cleanly remove router mappings on graceful server shutdown
			cleanCtx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
			m.removeAllActive(cleanCtx)
			cancel()
			return
		case <-m.wake:
			m.Sync(ctx)
		case <-ticker.C:
			if m.cfg.Remote.UPnPEnabled {
				m.Sync(ctx)
			}
		}
	}
}

// Sync reconciles desired router port mappings with the active UPnP-IGD or NAT-PMP gateway.
func (m *Manager) Sync(ctx context.Context) Status {
	if !m.cfg.Remote.UPnPEnabled {
		m.removeAllActive(ctx)
		m.mu.Lock()
		m.status = Status{Enabled: false, Mappings: []Mapping{}}
		m.mu.Unlock()
		return m.Status()
	}

	desired := m.desiredMappings()
	if len(desired) == 0 {
		m.removeAllActive(ctx)
		m.mu.Lock()
		m.status = Status{Enabled: true, Mappings: []Mapping{}, LastError: "Enable at least one service (Party Mode or DJ Radio) to map"}
		m.mu.Unlock()
		return m.Status()
	}

	// 1. Try UPnP-IGD discovery first
	gw, err := m.discoverIGD(ctx)
	if err == nil && gw != nil {
		extIP, _ := m.igdGetExternalIP(ctx, gw)
		var mapped []Mapping
		var firstErr string
		for _, d := range desired {
			if merr := m.igdAddPortMapping(ctx, gw, d.ExternalPort, d.InternalPort, gw.localIP, "My Echo "+d.Service, 3600); merr != nil {
				d.Active = false
				d.Error = merr.Error()
				if firstErr == "" {
					firstErr = merr.Error()
				}
			} else {
				d.Active = true
				if extIP != "" {
					d.ExternalURL = fmt.Sprintf("http://%s:%d/", extIP, d.ExternalPort)
				}
			}
			mapped = append(mapped, d)
		}
		m.mu.Lock()
		m.igd = gw
		m.natPMPGW = nil
		m.status = Status{
			Enabled:     true,
			Method:      "upnp-igd",
			GatewayName: gw.name,
			ExternalIP:  extIP,
			Mappings:    mapped,
			LastError:   firstErr,
			LastRenewAt: time.Now().UTC().Format(time.RFC3339),
		}
		m.mu.Unlock()
		log.Printf("[netmap] UPnP-IGD synced via %s (WAN %s): %d mapping(s)", gw.name, extIP, len(mapped))
		return m.Status()
	}

	// 2. Fallback to NAT-PMP (RFC 6886, UDP 5351)
	if gwIP := guessDefaultGatewayIP(m.cfg.Remote.UPnPGatewayOverride, m.cfg.EffectiveAdvertiseHost()); gwIP != nil {
		extIP, pmpErr := natPMPExternalIP(gwIP)
		if pmpErr == nil {
			var mapped []Mapping
			var firstErr string
			for _, d := range desired {
				gotExt, merr := natPMPMapTCP(gwIP, d.InternalPort, d.ExternalPort, 3600)
				if merr != nil {
					d.Active = false
					d.Error = merr.Error()
					if firstErr == "" {
						firstErr = merr.Error()
					}
				} else {
					d.Active = true
					d.ExternalPort = gotExt
					d.ExternalURL = fmt.Sprintf("http://%s:%d/", extIP, gotExt)
				}
				mapped = append(mapped, d)
			}
			m.mu.Lock()
			m.igd = nil
			m.natPMPGW = gwIP
			m.status = Status{
				Enabled:     true,
				Method:      "nat-pmp",
				GatewayName: gwIP.String(),
				ExternalIP:  extIP,
				Mappings:    mapped,
				LastError:   firstErr,
				LastRenewAt: time.Now().UTC().Format(time.RFC3339),
			}
			m.mu.Unlock()
			log.Printf("[netmap] NAT-PMP synced via %s (WAN %s): %d mapping(s)", gwIP, extIP, len(mapped))
			return m.Status()
		}
	}

	m.mu.Lock()
	m.status = Status{
		Enabled:   true,
		Mappings:  desired,
		LastError: fmt.Sprintf("No UPnP-IGD or NAT-PMP router gateway found (%v). Ensure UPnP/NAT-PMP is enabled on your router and Docker runs with --network host (or use Cloudflare Tunnel instead).", err),
	}
	m.mu.Unlock()
	return m.Status()
}

func (m *Manager) desiredMappings() []Mapping {
	var out []Mapping
	if m.cfg.Remote.UPnPExposeParty {
		ext := m.cfg.Remote.UPnPExternalParty
		if ext <= 0 {
			ext = m.cfg.PartyHTTPPort
		}
		out = append(out, Mapping{
			Service:      "Party Mode",
			Protocol:     "TCP",
			InternalPort: m.cfg.PartyHTTPPort,
			ExternalPort: ext,
		})
	}
	if m.cfg.Remote.UPnPExposeDJ {
		ext := m.cfg.Remote.UPnPExternalDJ
		if ext <= 0 {
			ext = m.cfg.APIPort
		}
		out = append(out, Mapping{
			Service:      "DJ Radio & Web UI",
			Protocol:     "TCP",
			InternalPort: m.cfg.APIPort,
			ExternalPort: ext,
		})
	}
	return out
}

func (m *Manager) removeAllActive(ctx context.Context) {
	m.mu.RLock()
	gw := m.igd
	pmpGW := m.natPMPGW
	mappings := append([]Mapping(nil), m.status.Mappings...)
	m.mu.RUnlock()

	for _, mp := range mappings {
		if !mp.Active {
			continue
		}
		if gw != nil {
			_ = m.igdDeletePortMapping(ctx, gw, mp.ExternalPort)
		} else if pmpGW != nil {
			_, _ = natPMPMapTCP(pmpGW, mp.InternalPort, 0, 0)
		}
	}
}

// --- UPnP-IGD Discovery & SOAP ---

func (m *Manager) discoverIGD(ctx context.Context) (*igdGateway, error) {
	if override := strings.TrimSpace(m.cfg.Remote.UPnPGatewayOverride); strings.HasPrefix(override, "http://") || strings.HasPrefix(override, "https://") {
		if gw, ok := m.fetchIGDDescription(ctx, override); ok {
			return gw, nil
		}
	}

	conn, err := net.ListenPacket("udp4", ":0")
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	dst, _ := net.ResolveUDPAddr("udp4", ssdpAddr)

	targets := []string{
		"urn:schemas-upnp-org:device:InternetGatewayDevice:1",
		"urn:schemas-upnp-org:device:InternetGatewayDevice:2",
		"urn:schemas-upnp-org:service:WANIPConnection:1",
		"urn:schemas-upnp-org:service:WANPPPConnection:1",
	}
	for _, st := range targets {
		msg := fmt.Sprintf("M-SEARCH * HTTP/1.1\r\nHOST: %s\r\nMAN: \"ssdp:discover\"\r\nMX: 2\r\nST: %s\r\n\r\n", ssdpAddr, st)
		_, _ = conn.WriteTo([]byte(msg), dst)
	}

	_ = conn.SetReadDeadline(time.Now().Add(2200 * time.Millisecond))
	seen := make(map[string]bool)
	buf := make([]byte, 4096)
	for {
		n, _, rerr := conn.ReadFrom(buf)
		if rerr != nil {
			break
		}
		if loc := extractHeader(string(buf[:n]), "location"); loc != "" && !seen[loc] {
			seen[loc] = true
			if gw, ok := m.fetchIGDDescription(ctx, loc); ok {
				return gw, nil
			}
		}
	}
	return nil, fmt.Errorf("no UPnP-IGD response on LAN")
}

type igdDoc struct {
	URLBase string       `xml:"URLBase"`
	Device  igdDeviceTag `xml:"device"`
}
type igdDeviceTag struct {
	FriendlyName string         `xml:"friendlyName"`
	Services     []igdService   `xml:"serviceList>service"`
	Devices      []igdDeviceTag `xml:"deviceList>device"`
}
type igdService struct {
	ServiceType string `xml:"serviceType"`
	ControlURL  string `xml:"controlURL"`
}

func (m *Manager) fetchIGDDescription(ctx context.Context, loc string) (*igdGateway, bool) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, loc, nil)
	if err != nil {
		return nil, false
	}
	resp, err := m.httpClient.Do(req)
	if err != nil {
		return nil, false
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 524_288))
	if err != nil {
		return nil, false
	}
	var doc igdDoc
	if err := xml.Unmarshal(raw, &doc); err != nil {
		return nil, false
	}
	baseURL, err := url.Parse(loc)
	if err != nil {
		return nil, false
	}
	if strings.TrimSpace(doc.URLBase) != "" {
		if ub, err := url.Parse(strings.TrimSpace(doc.URLBase)); err == nil {
			baseURL = ub
		}
	}

	var ctrlURL, svcType string
	var walk func(d igdDeviceTag)
	walk = func(d igdDeviceTag) {
		for _, s := range d.Services {
			if ctrlURL == "" && (strings.Contains(s.ServiceType, "WANIPConnection") || strings.Contains(s.ServiceType, "WANPPPConnection")) {
				if ref, err := url.Parse(strings.TrimSpace(s.ControlURL)); err == nil {
					ctrlURL = baseURL.ResolveReference(ref).String()
					svcType = strings.TrimSpace(s.ServiceType)
				}
			}
		}
		for _, sub := range d.Devices {
			walk(sub)
		}
	}
	walk(doc.Device)
	if ctrlURL == "" {
		return nil, false
	}

	localIP := m.cfg.EffectiveAdvertiseHost()
	if c, err := net.DialTimeout("tcp", baseURL.Host, 2*time.Second); err == nil {
		if ta, ok := c.LocalAddr().(*net.TCPAddr); ok && ta.IP != nil {
			localIP = ta.IP.String()
		}
		_ = c.Close()
	}

	name := strings.TrimSpace(doc.Device.FriendlyName)
	if name == "" {
		name = baseURL.Host
	}
	return &igdGateway{name: name, controlURL: ctrlURL, serviceType: svcType, localIP: localIP}, true
}

func (m *Manager) igdSOAP(ctx context.Context, gw *igdGateway, action, bodyXML string) ([]byte, error) {
	env := fmt.Sprintf(`<?xml version="1.0"?><s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/"><s:Body>%s</s:Body></s:Envelope>`, bodyXML)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, gw.controlURL, bytes.NewBufferString(env))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", `text/xml; charset="utf-8"`)
	req.Header.Set("SOAPACTION", fmt.Sprintf(`"%s#%s"`, gw.serviceType, action))
	resp, err := m.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 65536))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return raw, fmt.Errorf("UPnP %s HTTP %d", action, resp.StatusCode)
	}
	return raw, nil
}

func (m *Manager) igdGetExternalIP(ctx context.Context, gw *igdGateway) (string, error) {
	raw, err := m.igdSOAP(ctx, gw, "GetExternalIPAddress", fmt.Sprintf(`<u:GetExternalIPAddress xmlns:u="%s"/>`, gw.serviceType))
	if err != nil {
		return "", err
	}
	s := string(raw)
	const openTag, closeTag = "<NewExternalIPAddress>", "</NewExternalIPAddress>"
	if a := strings.Index(s, openTag); a >= 0 {
		s = s[a+len(openTag):]
		if b := strings.Index(s, closeTag); b >= 0 {
			return strings.TrimSpace(s[:b]), nil
		}
	}
	return "", fmt.Errorf("no external IP in UPnP response")
}

func (m *Manager) igdAddPortMapping(ctx context.Context, gw *igdGateway, extPort, intPort int, intClient, desc string, leaseSec int) error {
	body := fmt.Sprintf(`<u:AddPortMapping xmlns:u="%s"><NewRemoteHost></NewRemoteHost><NewExternalPort>%d</NewExternalPort><NewProtocol>TCP</NewProtocol><NewInternalPort>%d</NewInternalPort><NewInternalClient>%s</NewInternalClient><NewEnabled>1</NewEnabled><NewPortMappingDescription>%s</NewPortMappingDescription><NewLeaseDuration>%d</NewLeaseDuration></u:AddPortMapping>`,
		gw.serviceType, extPort, intPort, intClient, desc, leaseSec)
	_, err := m.igdSOAP(ctx, gw, "AddPortMapping", body)
	return err
}

func (m *Manager) igdDeletePortMapping(ctx context.Context, gw *igdGateway, extPort int) error {
	body := fmt.Sprintf(`<u:DeletePortMapping xmlns:u="%s"><NewRemoteHost></NewRemoteHost><NewExternalPort>%d</NewExternalPort><NewProtocol>TCP</NewProtocol></u:DeletePortMapping>`,
		gw.serviceType, extPort)
	_, err := m.igdSOAP(ctx, gw, "DeletePortMapping", body)
	return err
}

// --- NAT-PMP (RFC 6886) Fallback ---

func guessDefaultGatewayIP(override, localIP string) net.IP {
	if ip := net.ParseIP(strings.TrimSpace(override)); ip != nil {
		return ip.To4()
	}
	if ip := net.ParseIP(strings.TrimSpace(localIP)); ip != nil {
		if v4 := ip.To4(); v4 != nil {
			return net.IPv4(v4[0], v4[1], v4[2], 1)
		}
	}
	return nil
}

func natPMPExternalIP(gw net.IP) (string, error) {
	conn, err := net.DialUDP("udp4", nil, &net.UDPAddr{IP: gw, Port: 5351})
	if err != nil {
		return "", err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(1500 * time.Millisecond))
	if _, err := conn.Write([]byte{0, 0}); err != nil {
		return "", err
	}
	resp := make([]byte, 16)
	n, err := conn.Read(resp)
	if err != nil || n < 12 {
		return "", fmt.Errorf("NAT-PMP external IP timeout")
	}
	if binary.BigEndian.Uint16(resp[2:4]) != 0 {
		return "", fmt.Errorf("NAT-PMP error code %d", binary.BigEndian.Uint16(resp[2:4]))
	}
	return net.IPv4(resp[8], resp[9], resp[10], resp[11]).String(), nil
}

func natPMPMapTCP(gw net.IP, internalPort, externalPort, leaseSec int) (int, error) {
	conn, err := net.DialUDP("udp4", nil, &net.UDPAddr{IP: gw, Port: 5351})
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(1500 * time.Millisecond))
	pkt := make([]byte, 12)
	pkt[0] = 0 // version
	pkt[1] = 2 // opcode: map TCP
	binary.BigEndian.PutUint16(pkt[4:6], uint16(internalPort))
	binary.BigEndian.PutUint16(pkt[6:8], uint16(externalPort))
	binary.BigEndian.PutUint32(pkt[8:12], uint32(leaseSec))
	if _, err := conn.Write(pkt); err != nil {
		return 0, err
	}
	resp := make([]byte, 16)
	n, err := conn.Read(resp)
	if err != nil || n < 16 {
		return 0, fmt.Errorf("NAT-PMP map timeout")
	}
	if code := binary.BigEndian.Uint16(resp[2:4]); code != 0 {
		return 0, fmt.Errorf("NAT-PMP map refused (code %d)", code)
	}
	return int(binary.BigEndian.Uint16(resp[10:12])), nil
}

func extractHeader(pkt, key string) string {
	key = strings.ToLower(key) + ":"
	sc := bufio.NewScanner(strings.NewReader(pkt))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if strings.HasPrefix(strings.ToLower(line), key) {
			return strings.TrimSpace(line[len(key):])
		}
	}
	return ""
}
