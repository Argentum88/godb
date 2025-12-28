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
	slottedPage.Init()

	tuple1 := make([]byte, 5)
	rnd.Read(tuple1)
	slottedPage.InsertTuple(tuple1)

	tuple2 := make([]byte, 5)
	rnd.Read(tuple2)
	id, _ := slottedPage.InsertTuple(tuple2)

	tuple3 := make([]byte, 5)
	rnd.Read(tuple3)
	slottedPage.InsertTuple(tuple3)

	tuple, _ := slottedPage.GetTuple(id)
	if !bytes.Equal(tuple, tuple2) {
		t.Fatalf("expected %v, got %v", tuple2, tuple)
	}
}

func Test_slottedPage_compact(t *testing.T) {
	t.Parallel()

	pageData := make([]byte, 70)
	sp := NewSlottedPage(pageData)
	sp.Init()

	tupleA := bytes.Repeat([]byte{0xAA}, 10)
	slotA, err := sp.InsertTuple(tupleA)
	if err != nil {
		t.Fatalf("insert tupleA: %v", err)
	}

	tupleB := bytes.Repeat([]byte{0xBB}, 10)
	slotB, err := sp.InsertTuple(tupleB)
	if err != nil {
		t.Fatalf("insert tupleB: %v", err)
	}

	sp.SetTupleAsUnused(slotA)

	tupleC := bytes.Repeat([]byte{0xCC}, 20)
	_, err = sp.InsertTuple(tupleC)
	if err != nil {
		t.Fatalf("insert tupleC: %v", err)
	}

	// Триггерим compaction т.к. contiguous free space уже мало, но total free space (с учетом дыр) достаточно.
	tupleD := bytes.Repeat([]byte{0xDD}, 20)
	_, err = sp.InsertTuple(tupleD)
	if err != nil {
		t.Fatalf("insert tupleD (should compact): %v", err)
	}

	// Проверяем, что tupleB не был поврежден после compaction
	gotB, err := sp.GetTuple(slotB)
	if err != nil {
		t.Fatalf("get tupleB: %v", err)
	}
	if !bytes.Equal(gotB, tupleB) {
		t.Fatalf("tupleB corrupted after compact: expected %v, got %v", tupleB, gotB)
	}
}

func Test_slottedPage_full(t *testing.T) {
	t.Parallel()
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	pageData := make([]byte, 20)
	sp := NewSlottedPage(pageData)
	sp.Init()

	tuple1 := make([]byte, 10)
	rnd.Read(tuple1)
	_, err := sp.InsertTuple(tuple1)
	if err != nil {
		t.Fatalf("insert tuple1: %v", err)
	}

	tuple2 := make([]byte, 10)
	rnd.Read(tuple2)
	_, err = sp.InsertTuple(tuple2)
	if err != ErrPageFull {
		t.Fatalf("expected PageFullErr, got %v", err)
	}
}
