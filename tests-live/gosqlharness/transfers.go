// Byte fixtures for exercising explicit backup SQL, not a client command API.
package gosqlharness

import (
	"encoding/base64"
	"fmt"

	goharness "github.com/marcgauthier/murmur-sql/tests-live/goharness"
)

// Download executes the supplied DOWNLOAD SQL with bounded ranges and
// checks offsets.
func Download(client *goharness.Client, statement string, output *[]byte) error {
	var offset int64
	for {
		rows, err := client.Query(fmt.Sprintf("%s OFFSET %d LIMIT %d", statement, offset, 1048576))
		if err != nil {
			return err
		}
		if len(rows) != 1 {
			return fmt.Errorf("murmur: protocol: expected one backup range")
		}
		actual, err := rows[0].Int64(0)
		if err != nil {
			return err
		}
		total, err := rows[0].Int64(1)
		if err != nil {
			return err
		}
		chunk, err := rows[0].Bytes(2)
		if err != nil {
			return err
		}
		if actual != offset || total < offset || int64(len(chunk)) > total-offset {
			return fmt.Errorf("murmur: protocol: invalid backup range")
		}
		*output = append(*output, chunk...)
		offset += int64(len(chunk))
		if offset == total {
			return nil
		}
		if len(chunk) == 0 {
			return fmt.Errorf("murmur: protocol: empty nonfinal backup range")
		}
	}
}

// Upload transfers a byte fixture using explicit backup SQL; it aborts
// on failure.
func Upload(client *goharness.Client, name string, data []byte) error {
	quoted := SQLLiteral(name)
	if _, err := client.Simple("MURMUR BACKUP UPLOAD BEGIN " + quoted); err != nil {
		return err
	}
	phase := func() error {
		for len(data) > 0 {
			n := 256 << 10
			if n > len(data) {
				n = len(data)
			}
			if _, err := client.Simple("MURMUR BACKUP UPLOAD APPEND " + quoted + " '" + base64.StdEncoding.EncodeToString(data[:n]) + "'"); err != nil {
				return err
			}
			data = data[n:]
		}
		_, err := client.Simple("MURMUR BACKUP UPLOAD FINISH " + quoted)
		return err
	}
	if err := phase(); err != nil {
		_, _ = client.Simple("MURMUR BACKUP UPLOAD ABORT " + quoted)
		return err
	}
	return nil
}
