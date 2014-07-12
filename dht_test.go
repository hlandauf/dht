package dht

import (
  "expvar"
  "flag"
  "fmt"
  "net"
  "strconv"
  "sync"
  "testing"
  "time"
  "strings"

  "github.com/nictuku/nettools"
)

func init() {
  // TestDHTLocal requires contacting the same nodes multiple times, so
  // shorten the retry period to make tests run faster.
  searchRetryPeriod = time.Second
}

// ExampleDHT is a simple example that searches for a particular infohash and
// exits when it finds any peers. A stand-alone version can be found in the
// examples/ directory.
func ExampleDHT() {
  if testing.Short() {
    fmt.Println("Peer found for the requested infohash or the test was skipped")
    return
  }
  d, err := New(nil)
  if err != nil {
    fmt.Println(err)
    return
  }
  go d.Run()

  infoHash, err := DecodeInfoHash("d1c5676ae7ac98e8b19f63565905105e3c4c37a2")
  if err != nil {
    fmt.Printf("DecodeInfoHash faiure: %v", err)
    return
  }

  tick := time.Tick(time.Second)

  var infoHashPeers map[InfoHash][]string
M:
  for {
    select {
    case <-tick:
      // Repeat the request until a result appears, querying nodes that haven't been
      // consulted before and finding close-by candidates for the infohash.
      d.PeersRequest(string(infoHash), false)
    case infoHashPeers = <-d.PeersRequestResults:
      break M
    case <-time.After(30 * time.Second):
      fmt.Printf("Could not find new peers: timed out")
      return
    }
  }
  for ih, peers := range infoHashPeers {
    if len(peers) > 0 {
      // Peers are encoded in binary format. Decoding example using github.com/nictuku/nettools:
      //for _, peer := range peers {
      //  fmt.Println(DecodePeerAddress(peer))
      //}

      if fmt.Sprintf("%x", ih) == "d1c5676ae7ac98e8b19f63565905105e3c4c37a2" {
        fmt.Println("Peer found for the requested infohash or the test was skipped")
        return
      }
    }
  }

  // Output:
  // Peer found for the requested infohash or the test was skipped
}

func startNode(routers string, ih string) (*DHT, error) {
  c := NewConfig()
  //c.SaveRoutingTable = false
  c.DHTRouters = strings.Split(routers, ",")
  if len(c.DHTRouters) == 1 && c.DHTRouters[0] == "" {
    c.DHTRouters = []string{}
  }
  c.Port = 0 //6060 //0
  node, err := New(c)
  if err != nil {
    return nil, err
  }
  // Remove the buffer
  node.peersRequest = make(chan ihReq, 0)
  go node.Run()
  node.PeersRequest(ih, true)
  return node, nil
}

// drainResults loops until the target number of peers are found, or a time limit is reached.
func drainResults(n *DHT, ih string, targetCount int, timeout time.Duration) error {
  count := 0
  after := time.After(timeout)
  tick := time.NewTicker(time.Second/5)
  defer tick.Stop()
  for {
    select {
    case r := <-n.PeersRequestResults:
      fmt.Println(":)")
      for _, peers := range r {
        for _, x := range peers {
          fmt.Printf("Found peer %d: %v\n", count, decodePeerAddress(x))
          count++
          if count >= targetCount {
            return nil
          }
        }
      }
    case <-after:
      return fmt.Errorf("drainResult timed out")

    case <-tick.C:
      n.PeersRequest(ih, true)
    }
  }
}

func TestDHTLocal(t *testing.T) {
  if testing.Short() {
    fmt.Println("Skipping TestDHTLocal")
    return
  }
  fmt.Println("A01")
  infoHash, err := DecodeInfoHash("d1c5676ae7ac98e8b19f63565905105e3c4c37a2")
  if err != nil {
    t.Fatalf(err.Error())
  }
  n1, err := startNode("", string(infoHash))
  if err != nil {
    t.Errorf("n1 startNode: %v", err)
    return
  }
  fmt.Println("A02")

  router := fmt.Sprintf("localhost:%d", n1.Port())
  n2, err := startNode(router, string(infoHash))
  if err != nil {
    t.Errorf("n2 startNode: %v", err)
    return
  }
  fmt.Println("A03")
  n3, err := startNode(router, string(infoHash))
  if err != nil {
    t.Errorf("n3 startNode: %v", err)
    return
  }
  // n2 and n3 should find each other.
  wg := new(sync.WaitGroup)
  wg.Add(2)
  go func() {
    if err := drainResults(n2, string(infoHash), 1, 10*time.Second); err != nil {
      t.Errorf("drainResult n2: %v", err)
    }
    wg.Done()
  }()
  go func() {
    if err := drainResults(n3, string(infoHash), 1, 10*time.Second); err != nil {
      t.Errorf("drainResult n3: %v", err)
    }
    wg.Done()
  }()
  fmt.Println("A04")
  wg.Wait()
  fmt.Println("A05")
  n1.Stop()
  n2.Stop()
  n3.Stop()
  fmt.Println("A06")
}

// Requires Internet access and can be flaky if the server or the internet is
// slow.
func TestDHTLarge(t *testing.T) {
  if testing.Short() {
    t.Skip("TestDHTLarge requires internet access and can be flaky. Skipping in short mode.")
  }
  defer stats(t)
  c := NewConfig()
  c.Port = 6060 // ...
  //c.SaveRoutingTable = false
  node, err := New(c)
  if err != nil {
    t.Fatalf("dht New: %v", err)
  }
  go node.Run()
  realDHTNodes := []string{
    "1.a.magnets.im",
    "router.utorrent.com",
  }
  for _, addr := range realDHTNodes {
    ip, err := net.LookupHost(addr)
    if err != nil {
      t.Error(err)
      continue
    }
    node.AddNode(ip[0] + ":6881")
  }

  node.AddNode("117.78.1.52:6891")
  node.AddNode("88.183.138.12:52804")

  // Test that we can reach at least one node.
  success := false
  var (
    reachable int
    v         expvar.Var
  )
  for i := 0; i < 10; i++ {
    v = expvar.Get("totalNodesReached")
    reachable, err = strconv.Atoi(v.String())
    if err != nil {
      t.Errorf("totalNodesReached conversion to int failed: %v", err)
      continue
    }
    if reachable > 0 {
      t.Logf("Contacted %d DHT nodes.", reachable)
      success = true
      break
    }
    time.Sleep(time.Second)
  }
  if !success {
    t.Fatal("No external DHT node could be contacted.")
  }

  // Test that we can find peers for a known torrent in a timely fashion.
  //
  // Torrent from: http://www.clearbits.net/torrents/244-time-management-for-anarchists-1
  //infoHash := InfoHash("\xb4\x62\xc0\xa8\xbc\xef\x1c\xe5\xbb\x56\xb9\xfd\xb8\xcf\x37\xff\xd0\x2f\x5f\x59")
  infoHash := InfoHash("\xc0\x66\x42\x34\xb4\x4f\x25\xde\x6c\xc7\xb5\x36\xa7\x98\xc6\x5f\x85\x80\x79\xcb")
  //c0 66 42 34 b4 4f 25 de 6c c7 b5 36 a7 98 c6 5f 85 80 79 cb
  go node.PeersRequest(string(infoHash), true)
  timeout := make(chan bool, 1)
  go func() {
    time.Sleep(30 * time.Second)
    timeout <- true
  }()
  var infoHashPeers map[InfoHash][]string
  select {
  case infoHashPeers = <-node.PeersRequestResults:
    t.Logf("Found %d peers.", len(infoHashPeers[infoHash]))
  case <-timeout:
    t.Fatal("Could not find new peers: timed out")
  }
  for ih, peers := range infoHashPeers {
    if infoHash != ih {
      t.Fatal("Unexpected infohash returned")
    }
    if len(peers) == 0 {
      t.Fatal("Could not find new torrent peers.")
    }
    for _, peer := range peers {
      t.Logf("peer found: %v", nettools.BinaryToDottedPort(peer))
    }
  }
}

func TestNewDHTConfig(t *testing.T) {
  c := NewConfig()
  c.Port = 6060
  c.NumTargetPeers = 10

  d, err := New(c)
  if err != nil {
    t.Fatalf("DHT failed to init with config: %v", err)
  }
  if d.config.Port != c.Port || d.config.NumTargetPeers != c.NumTargetPeers {
    t.Fatal("DHT not initialized with config")
  }
}

func TestRegisterFlags(t *testing.T) {
  c := &Config{
    DHTRouters:    []string{"example.router.com:6060"},
    MaxNodes:      2020,
    CleanupPeriod: time.Second,
    //SavePeriod:    time.Second * 2,
    RateLimit:     999,
  }
  RegisterFlags(c)
  if flag.Lookup("routers").DefValue != c.DHTRoutersToString() {
    t.Fatal("Incorrect routers flag")
  }
  if flag.Lookup("maxNodes").DefValue != strconv.FormatInt(int64(c.MaxNodes), 10) {
    t.Fatal("Incorrect maxNodes flag")
  }
  if flag.Lookup("cleanupPeriod").DefValue != c.CleanupPeriod.String() {
    t.Fatal("Incorrect cleanupPeriod flag")
  }
  /*if flag.Lookup("savePeriod").DefValue != c.SavePeriod.String() {
    t.Fatal("Incorrect routers flag")
  }*/
  if flag.Lookup("rateLimit").DefValue != strconv.FormatInt(c.RateLimit, 10) {
    t.Fatal("Incorrect routers flag")
  }
}

func stats(t *testing.T) {
  t.Logf("=== Stats ===")
  t.Logf("totalNodesReached: %v", totalNodesReached)
  t.Logf("totalGetPeersDupes: %v", totalGetPeersDupes)
  t.Logf("totalFindNodeDupes: %v", totalFindNodeDupes)
  t.Logf("totalPeers: %v", totalPeers)
  t.Logf("totalSentFindNode: %v", totalSentFindNode)
  t.Logf("totalSentGetPeers: %v", totalSentGetPeers)
}
