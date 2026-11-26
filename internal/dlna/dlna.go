package dlna

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/baardove/my-echo/server/internal/config"
)

const ssdpAddr = "239.255.255.250:1900"

// Renderer represents a discovered DLNA / UPnP MediaRenderer (speaker, AVR, WiiM, Sonos, smart TV).
type Renderer struct {
	ID              string `json:"id"`
	Name            string `json:"name"`
	Manufacturer    string `json:"manufacturer,omitempty"`
	Model           string `json:"model,omitempty"`
	Location        string `json:"location"`
	AVTransportURL  string `json:"avTransportURL"`
	RenderCtrlURL   string `json:"renderingControlURL,omitempty"`
	ActiveChannel   string `json:"activeChannel,omitempty"` // "dj" | "party" | ""
}

type Status struct {
	Enabled      bool       `json:"enabled"`
	FriendlyName string     `json:"friendlyName"`
	UDN          string     `json:"udn"`
	DeviceXMLURL string     `json:"deviceXMLURL"`
	Renderers    []Renderer `json:"renderers"`
	LastScanAt   string     `json:"lastScanAt,omitempty"`
}

type Manager struct {
	cfg        *config.Config
	httpClient *http.Client
	udn        string

	mu         sync.RWMutex
	renderers  map[string]Renderer
	lastScanAt time.Time
	started    bool
}

func NewManager(cfg *config.Config) *Manager {
	sum := sha1.Sum([]byte("my-echo-dlna:" + cfg.ServerName + ":" + cfg.PairingPIN))
	hx := hex.EncodeToString(sum[:16])
	udn := fmt.Sprintf("uuid:%s-%s-%s-%s-%s", hx[0:8], hx[8:12], hx[12:16], hx[16:20], hx[20:32])
	return &Manager{
		cfg:        cfg,
		httpClient: &http.Client{Timeout: 5 * time.Second},
		udn:        udn,
		renderers:  make(map[string]Renderer),
	}
}

func (m *Manager) FriendlyName() string {
	if strings.TrimSpace(m.cfg.DLNA.FriendlyName) != "" {
		return strings.TrimSpace(m.cfg.DLNA.FriendlyName)
	}
	return fmt.Sprintf("My Echo Radio (%s)", m.cfg.ServerName)
}

func (m *Manager) Status() Status {
	m.mu.RLock()
	list := make([]Renderer, 0, len(m.renderers))
	for _, r := range m.renderers {
		list = append(list, r)
	}
	scanAt := m.lastScanAt
	m.mu.RUnlock()
	sort.Slice(list, func(i, j int) bool { return strings.ToLower(list[i].Name) < strings.ToLower(list[j].Name) })
	var scanStr string
	if !scanAt.IsZero() {
		scanStr = scanAt.UTC().Format(time.RFC3339)
	}
	return Status{
		Enabled:      m.cfg.DLNA.Enabled,
		FriendlyName: m.FriendlyName(),
		UDN:          m.udn,
		DeviceXMLURL: fmt.Sprintf("http://%s:%d/dlna/device.xml", m.cfg.EffectiveAdvertiseHost(), m.cfg.APIPort),
		Renderers:    list,
		LastScanAt:   scanStr,
	}
}

// Start runs the SSDP MediaServer responder & periodic NOTIFY broadcaster + initial renderer discovery.
func (m *Manager) Start(ctx context.Context) {
	m.mu.Lock()
	if m.started {
		m.mu.Unlock()
		return
	}
	m.started = true
	m.mu.Unlock()

	go m.ssdpResponderLoop(ctx)
	go m.ssdpNotifyLoop(ctx)
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-time.After(3 * time.Second):
		}
		if m.cfg.DLNA.Enabled {
			_, _ = m.DiscoverRenderers(ctx, 2*time.Second)
		}
	}()
}

// DiscoverRenderers sends SSDP M-SEARCH packets on the LAN to locate DLNA speakers, AVRs, and smart TVs.
func (m *Manager) DiscoverRenderers(ctx context.Context, wait time.Duration) ([]Renderer, error) {
	if wait <= 0 {
		wait = 2 * time.Second
	}
	conn, err := net.ListenPacket("udp4", ":0")
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	dst, err := net.ResolveUDPAddr("udp4", ssdpAddr)
	if err != nil {
		return nil, err
	}

	targets := []string{
		"urn:schemas-upnp-org:device:MediaRenderer:1",
		"urn:schemas-upnp-org:service:AVTransport:1",
	}
	for _, st := range targets {
		msg := fmt.Sprintf("M-SEARCH * HTTP/1.1\r\nHOST: %s\r\nMAN: \"ssdp:discover\"\r\nMX: 2\r\nST: %s\r\n\r\n", ssdpAddr, st)
		_, _ = conn.WriteTo([]byte(msg), dst)
	}

	_ = conn.SetReadDeadline(time.Now().Add(wait))
	locations := make(map[string]bool)
	buf := make([]byte, 4096)
	for {
		n, _, rerr := conn.ReadFrom(buf)
		if rerr != nil {
			break
		}
		if loc := extractSSDPHeader(string(buf[:n]), "location"); loc != "" {
			locations[loc] = true
		}
	}

	found := make(map[string]Renderer)
	var wg sync.WaitGroup
	var fmu sync.Mutex
	for loc := range locations {
		wg.Add(1)
		go func(u string) {
			defer wg.Done()
			if r, ok := m.fetchRendererDescription(ctx, u); ok {
				fmu.Lock()
				found[r.ID] = r
				fmu.Unlock()
			}
		}(loc)
	}
	wg.Wait()

	m.mu.Lock()
	for id, r := range found {
		if prev, ok := m.renderers[id]; ok {
			r.ActiveChannel = prev.ActiveChannel
		}
		m.renderers[id] = r
	}
	m.lastScanAt = time.Now().UTC()
	out := make([]Renderer, 0, len(m.renderers))
	for _, r := range m.renderers {
		out = append(out, r)
	}
	m.mu.Unlock()

	sort.Slice(out, func(i, j int) bool { return strings.ToLower(out[i].Name) < strings.ToLower(out[j].Name) })
	return out, nil
}

// CastStream tells a DLNA MediaRenderer to play My Echo's live DJ Radio or Party Mode MP3 stream.
func (m *Manager) CastStream(ctx context.Context, rendererID, channel string) (Renderer, error) {
	m.mu.RLock()
	r, ok := m.renderers[rendererID]
	m.mu.RUnlock()
	if !ok {
		return Renderer{}, fmt.Errorf("DLNA renderer %q not found — click Scan LAN first", rendererID)
	}
	if r.AVTransportURL == "" {
		return Renderer{}, fmt.Errorf("renderer %q does not expose AVTransport control", r.Name)
	}

	base := fmt.Sprintf("http://%s:%d", m.cfg.EffectiveAdvertiseHost(), m.cfg.APIPort)
	var streamURL, title string
	if strings.EqualFold(channel, "party") {
		channel = "party"
		streamURL = base + "/stream/party.mp3"
		title = m.cfg.ServerName + " · Party Mode Live"
	} else {
		channel = "dj"
		streamURL = base + "/stream/dj.mp3"
		station := m.cfg.DJ.RadioStationName
		if station == "" {
			station = "Echo FM"
		}
		title = station + " · Live AI DJ Radio"
	}

	didl := fmt.Sprintf(`<DIDL-Lite xmlns="urn:schemas-upnp-org:metadata-1-0/DIDL-Lite/" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:upnp="urn:schemas-upnp-org:metadata-1-0/upnp/"><item id="echo-%s" parentID="0" restricted="1"><dc:title>%s</dc:title><upnp:class>object.item.audioItem.audioBroadcast</upnp:class><res protocolInfo="http-get:*:audio/mpeg:DLNA.ORG_PN=MP3;DLNA.ORG_OP=00;DLNA.ORG_CI=1;DLNA.ORG_FLAGS=01700000000000000000000000000000">%s</res></item></DIDL-Lite>`,
		channel, xmlEsc(title), xmlEsc(streamURL))

	setURI := fmt.Sprintf(`<u:SetAVTransportURI xmlns:u="urn:schemas-upnp-org:service:AVTransport:1"><InstanceID>0</InstanceID><CurrentURI>%s</CurrentURI><CurrentURIMetaData>%s</CurrentURIMetaData></u:SetAVTransportURI>`,
		xmlEsc(streamURL), xmlEsc(didl))
	if err := m.soapAction(ctx, r.AVTransportURL, "urn:schemas-upnp-org:service:AVTransport:1", "SetAVTransportURI", setURI); err != nil {
		return Renderer{}, fmt.Errorf("SetAVTransportURI failed on %s: %w", r.Name, err)
	}

	playBody := `<u:Play xmlns:u="urn:schemas-upnp-org:service:AVTransport:1"><InstanceID>0</InstanceID><Speed>1</Speed></u:Play>`
	if err := m.soapAction(ctx, r.AVTransportURL, "urn:schemas-upnp-org:service:AVTransport:1", "Play", playBody); err != nil {
		return Renderer{}, fmt.Errorf("Play failed on %s: %w", r.Name, err)
	}

	m.mu.Lock()
	r.ActiveChannel = channel
	m.renderers[rendererID] = r
	m.mu.Unlock()
	log.Printf("[dlna] casting %s (%s) to %s", channel, streamURL, r.Name)
	return r, nil
}

// StopRenderer stops playback on a DLNA MediaRenderer.
func (m *Manager) StopRenderer(ctx context.Context, rendererID string) error {
	m.mu.RLock()
	r, ok := m.renderers[rendererID]
	m.mu.RUnlock()
	if !ok {
		return fmt.Errorf("DLNA renderer %q not found", rendererID)
	}
	stopBody := `<u:Stop xmlns:u="urn:schemas-upnp-org:service:AVTransport:1"><InstanceID>0</InstanceID></u:Stop>`
	if err := m.soapAction(ctx, r.AVTransportURL, "urn:schemas-upnp-org:service:AVTransport:1", "Stop", stopBody); err != nil {
		return err
	}
	m.mu.Lock()
	r.ActiveChannel = ""
	m.renderers[rendererID] = r
	m.mu.Unlock()
	return nil
}

// --- DLNA MediaServer HTTP Endpoints (/dlna/device.xml, /dlna/cds.xml, /dlna/control) ---

func (m *Manager) ServeDeviceXML(w http.ResponseWriter, r *http.Request) {
	host := m.cfg.EffectiveAdvertiseHost()
	base := fmt.Sprintf("http://%s:%d", host, m.cfg.APIPort)
	doc := fmt.Sprintf(`<?xml version="1.0" encoding="utf-8"?>
<root xmlns="urn:schemas-upnp-org:device-1-0" xmlns:dlna="urn:schemas-dlna-org:device-1-0">
  <specVersion><major>1</major><minor>0</minor></specVersion>
  <device>
    <deviceType>urn:schemas-upnp-org:device:MediaServer:1</deviceType>
    <friendlyName>%s</friendlyName>
    <manufacturer>My Echo</manufacturer>
    <modelName>My Echo Server</modelName>
    <modelNumber>2.0</modelNumber>
    <UDN>%s</UDN>
    <dlna:X_DLNADOC>DMS-1.50</dlna:X_DLNADOC>
    <iconList>
      <icon>
        <mimetype>image/png</mimetype>
        <width>128</width><height>128</height><depth>24</depth>
        <url>/icon.png</url>
      </icon>
    </iconList>
    <serviceList>
      <service>
        <serviceType>urn:schemas-upnp-org:service:ContentDirectory:1</serviceType>
        <serviceId>urn:upnp-org:serviceId:ContentDirectory</serviceId>
        <SCPDURL>/dlna/cds.xml</SCPDURL>
        <controlURL>/dlna/control</controlURL>
        <eventSubURL>/dlna/event</eventSubURL>
      </service>
    </serviceList>
    <presentationURL>%s/</presentationURL>
  </device>
</root>`, xmlEsc(m.FriendlyName()), xmlEsc(m.udn), xmlEsc(base))
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	_, _ = w.Write([]byte(doc))
}

func (m *Manager) ServeCDSXML(w http.ResponseWriter, r *http.Request) {
	const scpd = `<?xml version="1.0" encoding="utf-8"?>
<scpd xmlns="urn:schemas-upnp-org:service-1-0">
  <specVersion><major>1</major><minor>0</minor></specVersion>
  <actionList>
    <action><name>Browse</name>
      <argumentList>
        <argument><name>ObjectID</name><direction>in</direction><relatedStateVariable>A_ARG_TYPE_ObjectID</relatedStateVariable></argument>
        <argument><name>BrowseFlag</name><direction>in</direction><relatedStateVariable>A_ARG_TYPE_BrowseFlag</relatedStateVariable></argument>
        <argument><name>Filter</name><direction>in</direction><relatedStateVariable>A_ARG_TYPE_Filter</relatedStateVariable></argument>
        <argument><name>StartingIndex</name><direction>in</direction><relatedStateVariable>A_ARG_TYPE_Index</relatedStateVariable></argument>
        <argument><name>RequestedCount</name><direction>in</direction><relatedStateVariable>A_ARG_TYPE_Count</relatedStateVariable></argument>
        <argument><name>SortCriteria</name><direction>in</direction><relatedStateVariable>A_ARG_TYPE_SortCriteria</relatedStateVariable></argument>
        <argument><name>Result</name><direction>out</direction><relatedStateVariable>A_ARG_TYPE_Result</relatedStateVariable></argument>
        <argument><name>NumberReturned</name><direction>out</direction><relatedStateVariable>A_ARG_TYPE_Count</relatedStateVariable></argument>
        <argument><name>TotalMatches</name><direction>out</direction><relatedStateVariable>A_ARG_TYPE_Count</relatedStateVariable></argument>
        <argument><name>UpdateID</name><direction>out</direction><relatedStateVariable>A_ARG_TYPE_UpdateID</relatedStateVariable></argument>
      </argumentList>
    </action>
    <action><name>GetSystemUpdateID</name>
      <argumentList><argument><name>Id</name><direction>out</direction><relatedStateVariable>SystemUpdateID</relatedStateVariable></argument></argumentList>
    </action>
  </actionList>
  <serviceStateTable>
    <stateVariable sendEvents="no"><name>A_ARG_TYPE_ObjectID</name><dataType>string</dataType></stateVariable>
    <stateVariable sendEvents="no"><name>A_ARG_TYPE_BrowseFlag</name><dataType>string</dataType></stateVariable>
    <stateVariable sendEvents="no"><name>A_ARG_TYPE_Filter</name><dataType>string</dataType></stateVariable>
    <stateVariable sendEvents="no"><name>A_ARG_TYPE_Index</name><dataType>ui4</dataType></stateVariable>
    <stateVariable sendEvents="no"><name>A_ARG_TYPE_Count</name><dataType>ui4</dataType></stateVariable>
    <stateVariable sendEvents="no"><name>A_ARG_TYPE_SortCriteria</name><dataType>string</dataType></stateVariable>
    <stateVariable sendEvents="no"><name>A_ARG_TYPE_Result</name><dataType>string</dataType></stateVariable>
    <stateVariable sendEvents="no"><name>A_ARG_TYPE_UpdateID</name><dataType>ui4</dataType></stateVariable>
    <stateVariable sendEvents="yes"><name>SystemUpdateID</name><dataType>ui4</dataType></stateVariable>
  </serviceStateTable>
</scpd>`
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	_, _ = w.Write([]byte(scpd))
}

func (m *Manager) ServeControl(w http.ResponseWriter, r *http.Request) {
	soapAction := r.Header.Get("SOAPACTION")
	body, _ := io.ReadAll(io.LimitReader(r.Body, 65536))
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")

	if strings.Contains(soapAction, "GetSystemUpdateID") {
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="utf-8"?><s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/"><s:Body><u:GetSystemUpdateIDResponse xmlns:u="urn:schemas-upnp-org:service:ContentDirectory:1"><Id>1</Id></u:GetSystemUpdateIDResponse></s:Body></s:Envelope>`))
		return
	}

	base := fmt.Sprintf("http://%s:%d", m.cfg.EffectiveAdvertiseHost(), m.cfg.APIPort)
	station := m.cfg.DJ.RadioStationName
	if station == "" {
		station = "Echo FM"
	}
	isMeta := strings.Contains(string(body), "BrowseMetadata")
	var didl string
	count := 2
	if isMeta {
		count = 1
		didl = `<DIDL-Lite xmlns="urn:schemas-upnp-org:metadata-1-0/DIDL-Lite/" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:upnp="urn:schemas-upnp-org:metadata-1-0/upnp/"><container id="0" parentID="-1" restricted="1" childCount="2"><dc:title>` + xmlEsc(m.FriendlyName()) + `</dc:title><upnp:class>object.container</upnp:class></container></DIDL-Lite>`
	} else {
		proto := "http-get:*:audio/mpeg:DLNA.ORG_PN=MP3;DLNA.ORG_OP=00;DLNA.ORG_CI=1;DLNA.ORG_FLAGS=01700000000000000000000000000000"
		didl = fmt.Sprintf(`<DIDL-Lite xmlns="urn:schemas-upnp-org:metadata-1-0/DIDL-Lite/" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:upnp="urn:schemas-upnp-org:metadata-1-0/upnp/">`+
			`<item id="dj" parentID="0" restricted="1"><dc:title>%s · Live AI DJ Radio</dc:title><dc:creator>My Echo DJ</dc:creator><upnp:class>object.item.audioItem.audioBroadcast</upnp:class><res protocolInfo="%s">%s/stream/dj.mp3</res></item>`+
			`<item id="party" parentID="0" restricted="1"><dc:title>%s · Party Mode Live</dc:title><dc:creator>My Echo Party</dc:creator><upnp:class>object.item.audioItem.audioBroadcast</upnp:class><res protocolInfo="%s">%s/stream/party.mp3</res></item>`+
			`</DIDL-Lite>`,
			xmlEsc(station), proto, xmlEsc(base),
			xmlEsc(m.cfg.ServerName), proto, xmlEsc(base))
	}

	resp := fmt.Sprintf(`<?xml version="1.0" encoding="utf-8"?><s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/"><s:Body><u:BrowseResponse xmlns:u="urn:schemas-upnp-org:service:ContentDirectory:1"><Result>%s</Result><NumberReturned>%d</NumberReturned><TotalMatches>%d</TotalMatches><UpdateID>1</UpdateID></u:BrowseResponse></s:Body></s:Envelope>`,
		xmlEsc(didl), count, count)
	_, _ = w.Write([]byte(resp))
}

// --- SSDP Multicast Responder & Broadcaster ---

func (m *Manager) ssdpResponderLoop(ctx context.Context) {
	addr, err := net.ResolveUDPAddr("udp4", ssdpAddr)
	if err != nil {
		return
	}
	conn, err := net.ListenMulticastUDP("udp4", nil, addr)
	if err != nil {
		log.Printf("[dlna] SSDP multicast listener unavailable (use --network host on Docker for DLNA discovery): %v", err)
		return
	}
	defer conn.Close()
	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()

	buf := make([]byte, 4096)
	for {
		n, src, rerr := conn.ReadFromUDP(buf)
		if rerr != nil {
			if ctx.Err() != nil {
				return
			}
			continue
		}
		if !m.cfg.DLNA.Enabled {
			continue
		}
		pkt := string(buf[:n])
		if !strings.HasPrefix(pkt, "M-SEARCH ") {
			continue
		}
		st := extractSSDPHeader(pkt, "st")
		if st == "ssdp:all" || st == "upnp:rootdevice" ||
			strings.Contains(st, "MediaServer:1") || strings.Contains(st, "ContentDirectory:1") || st == m.udn {
			m.sendSSDPSearchResponse(conn, src, st)
		}
	}
}

func (m *Manager) sendSSDPSearchResponse(conn *net.UDPConn, dst *net.UDPAddr, st string) {
	loc := fmt.Sprintf("http://%s:%d/dlna/device.xml", m.cfg.EffectiveAdvertiseHost(), m.cfg.APIPort)
	if st == "ssdp:all" {
		st = "urn:schemas-upnp-org:device:MediaServer:1"
	}
	resp := fmt.Sprintf("HTTP/1.1 200 OK\r\nCACHE-CONTROL: max-age=1800\r\nDATE: %s\r\nEXT:\r\nLOCATION: %s\r\nSERVER: Linux/5.0 UPnP/1.1 MyEchoServer/2.0\r\nST: %s\r\nUSN: %s::%s\r\n\r\n",
		time.Now().UTC().Format(http.TimeFormat), loc, st, m.udn, st)
	_, _ = conn.WriteToUDP([]byte(resp), dst)
}

func (m *Manager) ssdpNotifyLoop(ctx context.Context) {
	send := func(nts string) {
		if !m.cfg.DLNA.Enabled && nts == "ssdp:alive" {
			return
		}
		conn, err := net.DialUDP("udp4", nil, &net.UDPAddr{IP: net.IPv4(239, 255, 255, 250), Port: 1900})
		if err != nil {
			return
		}
		defer conn.Close()
		loc := fmt.Sprintf("http://%s:%d/dlna/device.xml", m.cfg.EffectiveAdvertiseHost(), m.cfg.APIPort)
		ntsList := []string{
			"upnp:rootdevice",
			"urn:schemas-upnp-org:device:MediaServer:1",
			"urn:schemas-upnp-org:service:ContentDirectory:1",
		}
		for _, nt := range ntsList {
			pkt := fmt.Sprintf("NOTIFY * HTTP/1.1\r\nHOST: %s\r\nCACHE-CONTROL: max-age=1800\r\nLOCATION: %s\r\nNT: %s\r\nNTS: %s\r\nSERVER: Linux/5.0 UPnP/1.1 MyEchoServer/2.0\r\nUSN: %s::%s\r\n\r\n",
				ssdpAddr, loc, nt, nts, m.udn, nt)
			_, _ = conn.Write([]byte(pkt))
		}
	}

	send("ssdp:alive")
	ticker := time.NewTicker(4 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			send("ssdp:byebye")
			return
		case <-ticker.C:
			send("ssdp:alive")
		}
	}
}

// --- XML & SOAP Helpers ---

type upnpDeviceDoc struct {
	URLBase string        `xml:"URLBase"`
	Device  upnpDeviceTag `xml:"device"`
}
type upnpDeviceTag struct {
	FriendlyName string          `xml:"friendlyName"`
	Manufacturer string          `xml:"manufacturer"`
	ModelName    string          `xml:"modelName"`
	UDN          string          `xml:"UDN"`
	Services     []upnpService   `xml:"serviceList>service"`
	Devices      []upnpDeviceTag `xml:"deviceList>device"`
}
type upnpService struct {
	ServiceType string `xml:"serviceType"`
	ControlURL  string `xml:"controlURL"`
}

func (m *Manager) fetchRendererDescription(ctx context.Context, loc string) (Renderer, bool) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, loc, nil)
	if err != nil {
		return Renderer{}, false
	}
	resp, err := m.httpClient.Do(req)
	if err != nil {
		return Renderer{}, false
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 524_288))
	if err != nil {
		return Renderer{}, false
	}
	var doc upnpDeviceDoc
	if err := xml.Unmarshal(raw, &doc); err != nil {
		return Renderer{}, false
	}
	baseURL, err := url.Parse(loc)
	if err != nil {
		return Renderer{}, false
	}
	if strings.TrimSpace(doc.URLBase) != "" {
		if ub, err := url.Parse(strings.TrimSpace(doc.URLBase)); err == nil {
			baseURL = ub
		}
	}

	var avURL, rcURL string
	var walk func(d upnpDeviceTag)
	walk = func(d upnpDeviceTag) {
		for _, s := range d.Services {
			if strings.Contains(s.ServiceType, "AVTransport") && avURL == "" {
				if ref, err := url.Parse(strings.TrimSpace(s.ControlURL)); err == nil {
					avURL = baseURL.ResolveReference(ref).String()
				}
			}
			if strings.Contains(s.ServiceType, "RenderingControl") && rcURL == "" {
				if ref, err := url.Parse(strings.TrimSpace(s.ControlURL)); err == nil {
					rcURL = baseURL.ResolveReference(ref).String()
				}
			}
		}
		for _, sub := range d.Devices {
			walk(sub)
		}
	}
	walk(doc.Device)
	if avURL == "" {
		return Renderer{}, false
	}

	name := strings.TrimSpace(doc.Device.FriendlyName)
	if name == "" {
		name = baseURL.Host
	}
	id := strings.TrimSpace(doc.Device.UDN)
	if id == "" {
		sum := sha1.Sum([]byte(loc))
		id = hex.EncodeToString(sum[:8])
	}
	return Renderer{
		ID:             id,
		Name:           name,
		Manufacturer:   strings.TrimSpace(doc.Device.Manufacturer),
		Model:          strings.TrimSpace(doc.Device.ModelName),
		Location:       loc,
		AVTransportURL: avURL,
		RenderCtrlURL:  rcURL,
	}, true
}

func (m *Manager) soapAction(ctx context.Context, controlURL, serviceType, action, innerXML string) error {
	env := fmt.Sprintf(`<?xml version="1.0" encoding="utf-8"?><s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/"><s:Body>%s</s:Body></s:Envelope>`, innerXML)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, controlURL, bytes.NewBufferString(env))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", `text/xml; charset="utf-8"`)
	req.Header.Set("SOAPACTION", fmt.Sprintf(`"%s#%s"`, serviceType, action))
	resp, err := m.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("SOAP %s HTTP %d: %s", action, resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return nil
}

func extractSSDPHeader(pkt, key string) string {
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

func xmlEsc(s string) string {
	var buf bytes.Buffer
	_ = xml.EscapeText(&buf, []byte(s))
	return buf.String()
}
