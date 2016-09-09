package dtx

import (
	"testing"
	"time"
)

func TestTimeToStr(t *testing.T) {
	tm := time.Unix(1473108151, 968560000)
	tstr := timeToStr(tm)
	if tstr != "1473108151.968" {
		t.Errorf("wrong timeToStr %s", tstr)
	}

	tstrtm, err := strToTime(tstr)
	if err != nil {
		t.Fatalf("strToTime %v", err)
	}
	if tstrtm.Unix() != tm.Unix() {
		t.Errorf("wrong strToTime %v %v", tstrtm, tm)
	}
}
