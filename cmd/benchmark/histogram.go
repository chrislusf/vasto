// Copyright 2015 stevejiang. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package benchmark

import (
	"bytes"
	"fmt"
	"math"
)

const (
	kNumBuckets = 154
)

var kBucketLimit = [kNumBuckets]float64{
	1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 14, 16, 18, 20, 25, 30, 35, 40, 45,
	50, 60, 70, 80, 90, 100, 120, 140, 160, 180, 200, 250, 300, 350, 400, 450,
	500, 600, 700, 800, 900, 1000, 1200, 1400, 1600, 1800, 2000, 2500, 3000,
	3500, 4000, 4500, 5000, 6000, 7000, 8000, 9000, 10000, 12000, 14000,
	16000, 18000, 20000, 25000, 30000, 35000, 40000, 45000, 50000, 60000,
	70000, 80000, 90000, 100000, 120000, 140000, 160000, 180000, 200000,
	250000, 300000, 350000, 400000, 450000, 500000, 600000, 700000, 800000,
	900000, 1000000, 1200000, 1400000, 1600000, 1800000, 2000000, 2500000,
	3000000, 3500000, 4000000, 4500000, 5000000, 6000000, 7000000, 8000000,
	9000000, 10000000, 12000000, 14000000, 16000000, 18000000, 20000000,
	25000000, 30000000, 35000000, 40000000, 45000000, 50000000, 60000000,
	70000000, 80000000, 90000000, 100000000, 120000000, 140000000, 160000000,
	180000000, 200000000, 250000000, 300000000, 350000000, 400000000,
	450000000, 500000000, 600000000, 700000000, 800000000, 900000000,
	1000000000, 1200000000, 1400000000, 1600000000, 1800000000, 2000000000,
	2500000000.0, 3000000000.0, 3500000000.0, 4000000000.0, 4500000000.0,
	5000000000.0, 6000000000.0, 7000000000.0, 8000000000.0, 9000000000.0,
	1e200,
}

type histogram struct {
	min_         float64
	max_         float64
	num_         float64
	sum_         float64
	sum_squares_ float64

	buckets_ [kNumBuckets]float64
}

func (h *histogram) Clear() {
	h.min_ = kBucketLimit[kNumBuckets-1]
	h.max_ = 0
	h.num_ = 0
	h.sum_ = 0
	h.sum_squares_ = 0
	for i := 0; i < kNumBuckets; i++ {
		h.buckets_[i] = 0
	}
}

func (h *histogram) Add(value float64) {
	// Linear search is fast enough for our usage in db_bench
	var b = 0
	for b < kNumBuckets-1 && kBucketLimit[b] <= value {
		b++
	}
	h.buckets_[b] += 1.0
	if h.min_ > value {
		h.min_ = value
	}
	if h.max_ < value {
		h.max_ = value
	}
	h.num_++
	h.sum_ += value
	h.sum_squares_ += (value * value)
}

func (h *histogram) Merge(other *histogram) {
	if other.min_ < h.min_ {
		h.min_ = other.min_
	}
	if other.max_ > h.max_ {
		h.max_ = other.max_
	}
	h.num_ += other.num_
	h.sum_ += other.sum_
	h.sum_squares_ += other.sum_squares_
	for b := 0; b < kNumBuckets; b++ {
		h.buckets_[b] += other.buckets_[b]
	}
}

func (h *histogram) Median() float64 {
	return h.Percentile(50.0)
}

func (h *histogram) Percentile(p float64) float64 {
	var threshold = h.num_ * (p / 100.0)
	var sum float64 = 0
	for b := 0; b < kNumBuckets; b++ {
		sum += h.buckets_[b]
		if sum >= threshold {
			// Scale linearly within this bucket
			var left_point float64
			if b != 0 {
				left_point = kBucketLimit[b-1]
			}

			var right_point = kBucketLimit[b]
			var left_sum = sum - h.buckets_[b]
			var right_sum = sum
			var pos = (threshold - left_sum) / (right_sum - left_sum)
			var r = left_point + (right_point-left_point)*pos
			if r < h.min_ {
				r = h.min_
			}
			if r > h.max_ {
				r = h.max_
			}
			return r
		}
	}
	return h.max_
}

func (h *histogram) Average() float64 {
	if h.num_ == 0.0 {
		return 0
	}
	return h.sum_ / h.num_
}

func (h *histogram) StandardDeviation() float64 {
	if h.num_ == 0.0 {
		return 0
	}
	var variance = (h.sum_squares_*h.num_ - h.sum_*h.sum_) / (h.num_ * h.num_)
	return math.Sqrt(variance)
}

func (h *histogram) ToString() string {
	var s bytes.Buffer
	s.WriteString(fmt.Sprintf("Count: %.0f  Average: %.4f  StdDev: %.2f\n",
		h.num_, h.Average(), h.StandardDeviation()))

	var minRes float64
	if h.num_ != 0.0 {
		minRes = h.min_
	}
	s.WriteString(fmt.Sprintf("Min: %.4f  Median: %.4f  Max: %.4f\n",
		minRes, h.Median(), h.max_))
	s.WriteString("------------------------------------------------------\n")

	var mult = 100.0 / h.num_
	var sum float64
	for b := 0; b < kNumBuckets; b++ {
		if h.buckets_[b] <= 0.0 {
			continue
		}
		sum += h.buckets_[b]
		var leftRes float64
		if b != 0 {
			leftRes = kBucketLimit[b-1]
		}

		s.WriteString(fmt.Sprintf("[ %7.0f, %7.0f ) %7.0f %7.3f%% %7.3f%% ",
			leftRes,            // left
			kBucketLimit[b],    // right
			h.buckets_[b],      // count
			mult*h.buckets_[b], // percentage
			mult*sum))

		// Add hash marks based on percentage; 20 marks for 100%.
		var marks = int(20*(h.buckets_[b]/h.num_) + 0.5)
		for i := 0; i < marks; i++ {
			s.WriteByte('#')
		}
		s.WriteByte('\n')
	}

	return s.String()
}
