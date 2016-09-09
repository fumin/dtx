package dtx

import (
	cryptorand "crypto/rand"

	"github.com/btcsuite/btcutil/base58"
	"github.com/golang/glog"
)

func randString(n int) string {
	b := make([]byte, n)
	for {
		_, err := cryptorand.Read(b)
		if err == nil {
			break
		}
		glog.Warningf("%+v", err)
	}
	return base58.Encode(b)
}
