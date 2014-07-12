package dhtutil
import "database/sql"
import _ "github.com/mattn/go-sqlite3"
import "dht"
import "time"
import "encoding/hex"
import "crypto/rand"
import "path"
import "os"
import "strings"

func unused(x interface{}) {}

type DHTStorage struct {
  // Filename of the SQLite3 database to use.
  // Leave blank to use a sensible default.
  Filename string
}

func getUserLocalConfigDir(name string) string {
  d := "."
  env := os.Environ()
  for _, e := range env {
    if strings.HasPrefix(e, "HOME=") {
      d = path.Join(strings.SplitN(e, "=", 2)[1], ".cache")
    }
  }

  return path.Join(d, name)
}

func getDefaultFilename() string {
  dir := getUserLocalConfigDir("dht")

  os.MkdirAll(dir, 0750)
  return dir + "/dht.db"
}

func (ds *DHTStorage) getFilename() string {
  if ds.Filename == "" {
    ds.Filename = getDefaultFilename()
  }
  return ds.Filename
}

func (ds *DHTStorage) getDB() (*sql.DB, error) {
  return sql.Open("sqlite3", ds.getFilename())
}

func (ds *DHTStorage) createTablesWith(db *sql.DB) error {
  _, err := db.Exec(`

  CREATE TABLE IF NOT EXISTS dht_settings (
    key text NOT NULL,
    value text
  );
  CREATE UNIQUE INDEX IF NOT EXISTS idx_dht_settings__key ON dht_settings (key);

  CREATE TABLE IF NOT EXISTS dht_routers (
    id        integer    NOT NULL PRIMARY KEY,
    hostname  text       NOT NULL,
    node_id   text,
    first_seen timestamp,
    last_seen  timestamp,
    old       int        NOT NULL DEFAULT 0
  );
  CREATE UNIQUE INDEX IF NOT EXISTS idx_dht_routers__hostname ON dht_routers (hostname);

  `)

  return err
}

func (ds *DHTStorage) getPort() (v int, err error) {
  db, err := ds.getDB()
  if err != nil {
    return
  }
  defer db.Close()

  err = ds.createTablesWith(db)
  if err != nil {
    return
  }

  r, err := db.Query(`SELECT value FROM dht_settings WHERE key='port' LIMIT 1`)
  if err != nil {
    return
  }
  defer r.Close()

  for r.Next() {
    err = r.Scan(&v)
    if err != nil {
      return
    }

    return
  }

  return 0, nil
}

func (ds *DHTStorage) getNodeID() (nid string, err error) {
  db, err := ds.getDB()
  if err != nil {
    return
  }
  defer db.Close()

  err = ds.createTablesWith(db)
  if err != nil {
    return
  }

  r, err := db.Query(`SELECT value FROM dht_settings WHERE key='node_id' LIMIT 1`)
  if err != nil {
    return
  }
  defer r.Close()

  for r.Next() {
    var nid_s string
    err = r.Scan(&nid_s)
    if err != nil {
      return
    }

    var nid_ []byte
    nid_, err = hex.DecodeString(nid_s)
    if err != nil {
      return
    }
    nid = string(nid_)
    return
  }

  rb := make([]byte, 20)
  _, err = rand.Read(rb)
  if err != nil {
    return
  }

  _, err = db.Exec(`INSERT OR REPLACE INTO dht_settings (key,value) VALUES ('node_id',?)`,
    hex.EncodeToString(rb))
  if err != nil {
    return
  }

  nid = string(rb)

  return
}

// Loads the node ID and port number from the database and sets the values in the Config.
func (ds *DHTStorage) LoadDHTConfig(cfg *dht.Config) error {
  nid, err := ds.getNodeID()
  if err != nil {
    return err
  }

  port, err := ds.getPort()
  if err != nil {
    return err
  }

  cfg.NodeID = nid
  cfg.Port   = port

  return nil
}

// Loads the node database into the running DHT. Call this just after running the DHT.
func (ds *DHTStorage) LoadDHT(d *dht.DHT) error {
  db, err := ds.getDB()
  if err != nil {
    return err
  }
  defer db.Close()

  err = ds.createTablesWith(db)
  if err != nil {
    return err
  }

  r, err := db.Query(`SELECT hostname,node_id FROM dht_routers`)
  if err != nil {
    return err
  }
  defer r.Close()
  for r.Next() {
    var hostname string
    var node_id  interface{}
    err = r.Scan(&hostname, &node_id)
    if err != nil {
      return err
    }

    node_id_s, ok := node_id.([]byte)
    if !ok {
      node_id_s = []byte{}
    }

    node_id_bin, err := hex.DecodeString(string(node_id_s))
    if err != nil {
      node_id_bin = []byte{}
    }

    unused(node_id)
    d.AddNode(hostname, string(node_id_bin))
  }

  return nil
}

// Saves the node database into the DHT. Call this periodically or just before stopping the DHT.
// The current Node ID and port number are also saved.
func (ds *DHTStorage) SaveDHT(d *dht.DHT) error {
  db, err := ds.getDB()
  if err != nil {
    return err
  }

  defer db.Close()
  now := time.Now()

  err = ds.createTablesWith(db)
  if err != nil {
    return err
  }

  tx, err := db.Begin()
  if err != nil {
    return err
  }

  _, err = tx.Exec(`UPDATE dht_routers SET old=1;`)
  if err != nil {
    return err
  }

  err = d.VisitNodes(func (addr string, nodeID []byte) error {
    _, err := tx.Exec(`INSERT OR IGNORE INTO dht_routers
      (hostname,first_seen,old) VALUES (?,?,0);`,
      addr, now)
    if err != nil {
      return err
    }

    _, err = tx.Exec(`UPDATE dht_routers SET node_id=?, last_seen=?, old=0 WHERE hostname=?;`,
      hex.EncodeToString(nodeID), now, addr)
    if err != nil {
      return err
    }

    return nil
  })

  if err != nil {
    return err
  }

  _, err = tx.Exec(`DELETE FROM dht_routers WHERE old=1 AND last_seen < date('now','-2 hours');`)
  if err != nil {
    return err
  }

  _, err = tx.Exec(`UPDATE dht_routers SET first_seen=? WHERE first_seen IS NULL`, time.Now())

  if err != nil {
    return err
  }

  _, err = tx.Exec(`INSERT OR REPLACE INTO dht_settings (key,value) VALUES ('port',?)`, d.Port())
  if err != nil {
    return err
  }

  tx.Commit()
  return nil
}
