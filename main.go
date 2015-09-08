package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/ajstarks/svgo"

	_ "github.com/lib/pq"
)

const (
	defaultHost = "192.168.50.101"
)

var (
	c = NewController()
)

/*
select version();
                                                  version
-----------------------------------------------------------------------------------------------------------
 PostgreSQL 9.3.9 on x86_64-unknown-linux-gnu, compiled by gcc (Ubuntu 4.8.4-2ubuntu1~14.04) 4.8.4, 64-bit

  select pg_is_in_recovery() is a boolean to determine if it is a master

  select * from pg_stat_replication;
pid  | usesysid |  usename   | application_name |  client_addr   | client_hostname | client_port |         backend_start         |   state   | sent_location | write_location | flush_location | replay_location | sync_priority | sync_state
------+----------+------------+------------------+----------------+-----------------+-------------+-------------------------------+-----------+---------------+----------------+----------------+-----------------+---------------+------------
5103 |    16384 | replicator | walreceiver      | 192.168.50.104 |                 |       34592 | 2015-09-06 02:58:03.211148+00 | streaming | 0/37343C8     | 0/37343C8      | 0/37343C8      | 0/37343C8       |             0 | async

*/
type (
	controller struct {
		servers map[string]server
	}

	server struct {
		DBType   string               `json:"type"`
		Address  string               `json:"address"`           // address we connected to + port
		Follows  string               `json:"follows,omitempty"` //
		LastSeen time.Time            `json:"last_seen"`
		Clients  map[string]time.Time `json:"clients,omitempty"`
	}
)

// NewController initializes a new controller
func NewController() *controller {
	return &controller{
		servers: make(map[string]server),
	}
}

func (c *controller) WalkServers(address string, leader string) error {
	followers, err := c.NewServer(address, leader)
	if err != nil {
		return err
	}
	for _, follower := range followers {
		err = c.WalkServers(follower, address)
		if err != nil {
			return err
		}
	}
	return nil
}

// NewServer connects to a server
func (c *controller) NewServer(address string, follows string) ([]string, error) {
	var s server
	db, err := sql.Open("postgres", fmt.Sprintf("host=%s user=postgres dbname=postgres", address))
	if err != nil {
		log.Printf("error connecting to %s - %s\n", address, err)
		return nil, err
	}
	s.Follows = follows
	s.Address = address
	var inRecovery bool
	err = db.QueryRow("select pg_is_in_recovery();").Scan(&inRecovery)
	if err != nil {
		return nil, err
	}
	rows, err := db.Query("select client_addr from pg_stat_replication;")
	if err != nil {
		return nil, err
	}

	now := time.Now()
	s.LastSeen = now
	var followers []string
	for rows.Next() {
		var clientAddr string
		err = rows.Scan(&clientAddr)
		if err != nil {
			return nil, err
		}
		if s.Clients == nil {
			s.Clients = make(map[string]time.Time)
		}
		s.Clients[clientAddr] = now
		followers = append(followers, clientAddr)
		//	log.Printf("Adding client %s\n", clientAddr)
	}
	if !inRecovery {
		s.DBType = "master"
	} else {
		if len(s.Clients) > 0 {
			s.DBType = "relay"
		} else {
			s.DBType = "replica"
		}
	}
	c.servers[address] = s
	return followers, nil
}

func (c *controller) renderSVG(w http.ResponseWriter, r *http.Request) {

	type point struct {
		x int
		y int
	}

	nodes := make(map[string]point)
	w.Header().Set("Content-Type", "image/svg+xml")
	s := svg.New(w)
	width := 500
	height := 500
	s.Start(width, height)
	s.Title("Postgresql Replication")
	var relays int
	var replicas int
	circleSize := 50
	for name, val := range c.servers {
		switch val.DBType {
		case "master":
			x := 100
			y := 150
			s.Circle(x, y, circleSize, "fill:green;stroke:black")
			s.Text(x, y, name, "text-anchor:middle;font-size:15px;fill:white")
			nodes[name] = point{x, y}
		case "relay":
			relays++
			x := 100 + (150 * relays)
			y := 150
			s.Circle(x, y, circleSize, "fill:blue;stroke:black")
			s.Text(x, y, name, "text-anchor:middle;font-size:15px;fill:white")
			nodes[name] = point{x, y}
		case "replica":
			x := 100 + (150 * replicas)
			y := 300
			s.Circle(x, y, circleSize, "fill:red;stroke:black")
			s.Text(x, y, name, "text-anchor:middle;font-size:15px;fill:white")
			replicas++
			nodes[name] = point{x, y}
		default:
			log.Fatalf("invalid DBType found for %s %+v\n", name, val)
		}
	}
	for name, src := range nodes {
		switch c.servers[name].DBType {
		case "relay":
			dest := nodes[c.servers[name].Follows]
			s.Line(src.x-(circleSize), src.y, dest.x+(circleSize), dest.y, "stroke-width:1; stroke:black")
		case "replica":
			dest := nodes[c.servers[name].Follows]
			s.Line(src.x, src.y-circleSize, dest.x, dest.y+circleSize, "stroke-width:1; stroke:black")
		}
	}
	s.End()
}

func (c *controller) renderDOT(w http.ResponseWriter, r *http.Request) {
	buf := &bytes.Buffer{}
	buf.WriteString("digraph replication {\n")
	var color string
	for name, val := range c.servers {
		switch val.DBType {
		case "master":
			color = "green"
		case "relay":
			color = "blue"
		case "replica":
			color = "red"
		default:
			color = "black"
		}
		buf.WriteString(fmt.Sprintf("\"%s\" [color=%s];\n", name, color))
		if val.Follows != "" {
			buf.WriteString(fmt.Sprintf("\"%s\" -> \"%s\";\n", name, val.Follows))
		}
	}
	buf.WriteString("}\n")
	w.Write(buf.Bytes())
}
func (c *controller) mainHandler(w http.ResponseWriter, r *http.Request) {
	host := r.FormValue("host")
	if host == "" {
		host = defaultHost
	}
	c.WalkServers(host, "")
	render := r.FormValue("r")
	switch render {
	case "dot":
		c.renderDOT(w, r)
	case "svg":
		c.renderSVG(w, r)
	default:
		c.renderJSON(w, r)
	}
}
func (c *controller) renderJSON(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(c.servers)
}
func main() {
	c := NewController()
	http.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir("static"))))
	http.HandleFunc("/data", c.mainHandler)
	http.ListenAndServe(":8080", nil)
}
