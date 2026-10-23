package cli

import "crypto/rand"

func randomID() ([16]byte, error) { var b [16]byte; _, e := rand.Read(b[:]); return b, e }
