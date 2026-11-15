package tests

import (
	"fmt"
	"testing"

	"github.com/yoohooshantanu/dialpulse/internal/dnc"
	"github.com/yoohooshantanu/dialpulse/internal/model"
	"github.com/yoohooshantanu/dialpulse/internal/pacer"
)

func BenchmarkDNCBloomFilterLookup(b *testing.B) {
	bf := dnc.NewBloomFilter(1000000, 0.001)

	// Pre-populate 10,000 DNC records
	for i := 0; i < 10000; i++ {
		bf.Add(fmt.Sprintf("212555%04d", i))
	}

	testPhone := "2125551234"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = bf.Contains(testPhone)
	}
}

func BenchmarkPredictivePacerCalculation(b *testing.B) {
	camp := &model.Campaign{
		ID:                   "camp_bench",
		AllocatedAgents:      50,
		TargetAbandonRate:    0.025,
		MaxDialMultiplier:    3.5,
		HistoricalAnswerRate: 0.20,
		AverageHandleSec:     90.0,
		AverageRingSec:       15.0,
	}

	p := pacer.NewPredictivePacer(camp)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = p.CalculateDialsNeeded(15, 35)
	}
}
