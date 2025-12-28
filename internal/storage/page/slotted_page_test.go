package page

import (
	"bytes"
	"math/rand"
	"testing"
	"time"
)

func Test_slottedPage(t *testing.T) {
	t.Parallel()
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	pageData := make([]byte, 100)
	slottedPage := NewSlottedPage(pageData)
	slottedPage.Init(len(pageData))

	slot1 := make([]byte, 5)
	rnd.Read(slot1)
	slottedPage.InsertTuple(slot1)

	slot2 := make([]byte, 5)
	rnd.Read(slot2)
	id, _ := slottedPage.InsertTuple(slot2)

	slot3 := make([]byte, 5)
	rnd.Read(slot3)
	slottedPage.InsertTuple(slot3)

	slot, _ := slottedPage.GetTuple(id)
	if !bytes.Equal(slot, slot2) {
		t.Fatalf("expected %v, got %v", slot2, slot)
	}
}
