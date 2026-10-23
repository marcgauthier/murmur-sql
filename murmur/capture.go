package murmur

import (
	"github.com/marcgauthier/murmur-sql-go/murmur/internal/sqlite"
)

// Capture tracks one session over the managed tables.
type Capture struct {
	session *sqlite.Session
	tables  []string
}

// NewCapture attaches a session to tables on conn.
func NewCapture(conn *sqlite.Conn, tables []string) (*Capture, error) {
	s, err := conn.CreateSession()
	if err != nil {
		return nil, sqlErr(err)
	}
	cap := &Capture{session: s}
	for _, table := range tables {
		if err := s.Attach(table); err != nil {
			s.Close()
			return nil, sqlErr(err)
		}
		cap.tables = append(cap.tables, table)
	}
	return cap, nil
}

// Close deletes the session.
func (c *Capture) Close() {
	if c.session != nil {
		c.session.Close()
		c.session = nil
	}
}

// Size reports the pending changeset size in bytes.
func (c *Capture) Size() int {
	if c.session == nil {
		return 0
	}
	return int(c.session.ChangesetSize())
}

// Memory reports the session heap estimate in bytes.
func (c *Capture) Memory() int {
	if c.session == nil {
		return 0
	}
	return int(c.session.MemoryUsed())
}

// Bytes extracts the pending changeset.
func (c *Capture) Bytes() ([]byte, error) {
	if c.session == nil {
		return nil, nil
	}
	buf, err := c.session.Changeset()
	if err != nil {
		return nil, sqlErr(err)
	}
	return buf, nil
}

// Tables returns the attached table names.
func (c *Capture) Tables() []string { return c.tables }
