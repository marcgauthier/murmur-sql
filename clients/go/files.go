package murmur

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"sync"
	"time"
)

// FileInfo describes a logical file. Content is stored in encrypted disk objects.
type FileInfo struct {
	Name           string
	Size           int64
	ModifiedMillis int64
	SHA256         string
	Local          bool
}

// FileCursor owns a physical connection until exhausted or closed. Use Next,
// File, Err and Close as with database/sql.Rows. Calls must be serialized.
type FileCursor struct {
	mu    sync.Mutex
	stop  func() bool
	conn  *sql.Conn
	token string
	page  []FileInfo
	file  FileInfo
	err   error
}

// ListFiles opens a snapshot sorted by logical filename.
func (db *DB) ListFiles(ctx context.Context) (*FileCursor, error) {
	return db.fileCursor(ctx, "MURMUR FILE LIST", nil)
}

// SearchFiles opens a case-sensitive LIKE search (% and _; backslash escapes).
func (db *DB) SearchFiles(ctx context.Context, pattern string) (*FileCursor, error) {
	return db.fileCursor(ctx, "MURMUR FILE SEARCH $1", []any{pattern})
}
func (db *DB) fileCursor(ctx context.Context, query string, args []any) (*FileCursor, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	c := &FileCursor{conn: conn}
	if err = conn.QueryRowContext(ctx, query, args...).Scan(&c.token); err != nil {
		conn.Close()
		return nil, err
	}
	c.mu.Lock()
	c.stop = context.AfterFunc(ctx, func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.conn != nil {
			c.err = ctx.Err()
			c.closeLocked()
		}
	})
	c.mu.Unlock()
	return c, nil
}

// Next advances the cursor, fetching another bounded page when necessary.
func (c *FileCursor) Next(ctx context.Context) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil || c.err != nil {
		return false
	}
	if len(c.page) == 0 {
		rows, err := c.conn.QueryContext(ctx, "MURMUR FILE NEXT $1", c.token)
		if err != nil {
			c.err = err
			c.closeLocked()
			return false
		}
		for rows.Next() {
			var f FileInfo
			if err = rows.Scan(&f.Name, &f.Size, &f.ModifiedMillis, &f.SHA256, &f.Local); err != nil {
				break
			}
			c.page = append(c.page, f)
		}
		if err == nil {
			err = rows.Err()
		}
		rows.Close()
		if err != nil {
			c.err = err
			c.closeLocked()
			return false
		}
		if len(c.page) == 0 {
			c.err = c.closeLocked()
			return false
		}
	}
	c.file = c.page[0]
	c.page = c.page[1:]
	return true
}

// File returns the row selected by the last successful Next.
func (c *FileCursor) File() FileInfo { c.mu.Lock(); defer c.mu.Unlock(); return c.file }

// Err returns an iteration or cleanup error.
func (c *FileCursor) Err() error { c.mu.Lock(); defer c.mu.Unlock(); return c.err }

// Close releases server state and returns the pinned connection to the pool.
func (c *FileCursor) Close() error { c.mu.Lock(); defer c.mu.Unlock(); return c.closeLocked() }
func (c *FileCursor) closeLocked() error {
	if c.stop != nil {
		c.stop()
		c.stop = nil
	}
	if c.conn == nil {
		return nil
	}
	conn := c.conn
	c.conn = nil
	c.page = nil
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := conn.ExecContext(ctx, "MURMUR FILE CLOSE $1", c.token)
	closeErr := conn.Close()
	if err != nil {
		return err
	}
	return closeErr
}

// UploadFile streams bounded BYTEA chunks. Replacement becomes visible only
// after FINISH. Failure aborts staging and preserves the previously visible file.
func (db *DB) UploadFile(ctx context.Context, name string, r io.Reader) (FileInfo, error) {
	var info FileInfo
	conn, err := db.Conn(ctx)
	if err != nil {
		return info, err
	}
	defer conn.Close()
	var token string
	if err = conn.QueryRowContext(ctx, "MURMUR FILE UPLOAD BEGIN $1", name).Scan(&token); err != nil {
		return info, err
	}
	finished := false
	defer func() {
		if !finished {
			cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			conn.ExecContext(cleanup, "MURMUR FILE UPLOAD ABORT $1", token)
		}
	}()
	buf := make([]byte, 64<<10)
	emptyReads := 0
	for {
		n, readErr := r.Read(buf)
		if n > 0 {
			emptyReads = 0
			if _, err = conn.ExecContext(ctx, "MURMUR FILE UPLOAD APPEND $1 $2", token, buf[:n]); err != nil {
				return info, err
			}
		} else {
			emptyReads++
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return info, readErr
		}
		if emptyReads >= 100 {
			return info, io.ErrNoProgress
		}
		if err = ctx.Err(); err != nil {
			return info, err
		}
	}
	err = conn.QueryRowContext(ctx, "MURMUR FILE UPLOAD FINISH $1", token).Scan(&info.Name, &info.Size, &info.ModifiedMillis, &info.SHA256, &info.Local)
	finished = err == nil
	return info, err
}

// DownloadFile streams the pinned version into w. Remote-only content returns
// SQLSTATE 55000 so callers can retry after background peer fetching.
func (db *DB) DownloadFile(ctx context.Context, name string, w io.Writer) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	var token, actual string
	var size int64
	if err = conn.QueryRowContext(ctx, "MURMUR FILE DOWNLOAD $1", name).Scan(&token, &actual, &size); err != nil {
		return err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		conn.ExecContext(cleanup, "MURMUR FILE CLOSE $1", token)
	}()
	var position int64
	for {
		var offset, total int64
		var chunk []byte
		var eof bool
		if err = conn.QueryRowContext(ctx, "MURMUR FILE DOWNLOAD NEXT $1", token).Scan(&offset, &total, &chunk, &eof); err != nil {
			return err
		}
		if offset != position || total != size || int64(len(chunk)) > size-position || (!eof && len(chunk) == 0) {
			return fmt.Errorf("murmur: inconsistent file download")
		}
		n, writeErr := w.Write(chunk)
		if writeErr != nil {
			return writeErr
		}
		if n != len(chunk) {
			return io.ErrShortWrite
		}
		position += int64(n)
		if eof {
			if position != size {
				return io.ErrUnexpectedEOF
			}
			return nil
		}
	}
}
