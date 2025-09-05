package histogram

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/go-echarts/go-echarts/v2/charts"
	"github.com/go-echarts/go-echarts/v2/opts"
)

// HistogramConfig holds configuration for histogram generation
type HistogramConfig struct {
	NumBins      int     `json:"num_bins"`      // Number of bins for the histogram
	BinMethod    string  `json:"bin_method"`    // "auto", "sturges", "sqrt", "fd", or "fixed"
	MinValue     float64 `json:"min_value"`     // Minimum value for fixed bins (optional)
	MaxValue     float64 `json:"max_value"`     // Maximum value for fixed bins (optional)
	BinWidth     float64 `json:"bin_width"`     // Fixed bin width (optional)
	OutputDir    string  `json:"output_dir"`    // Directory for output files
	OutputPrefix string  `json:"output_prefix"` // Prefix for output files
}

// HistogramResult represents the computed histogram data
type HistogramResult struct {
	Config      HistogramConfig `json:"config"`
	Bins        []Bin           `json:"bins"`
	TotalCount  int64           `json:"total_count"`
	MinValue    float64         `json:"min_value"`
	MaxValue    float64         `json:"max_value"`
	Mean        float64         `json:"mean"`
	StdDev      float64         `json:"std_dev"`
	DataSummary DataSummary     `json:"data_summary"`
}

// Bin represents a single histogram bin
type Bin struct {
	LowerBound float64 `json:"lower_bound"`
	UpperBound float64 `json:"upper_bound"`
	Count      int64   `json:"count"`
}

// DataSummary provides statistical summary of the data
type DataSummary struct {
	Count    int64   `json:"count"`
	Sum      float64 `json:"sum"`
	Min      float64 `json:"min"`
	Max      float64 `json:"max"`
	Mean     float64 `json:"mean"`
	Variance float64 `json:"variance"`
	Skewness float64 `json:"skewness"`
	Kurtosis float64 `json:"kurtosis"`
}

// StreamingHistogram processes data streams to build histograms
type StreamingHistogram struct {
	config      HistogramConfig
	dataSummary DataSummary
	bins        []Bin
	binEdges    []float64
	initialized bool
	dataPoints  []float64 // Store data points for ECharts rendering
}

// DefaultConfig returns a default histogram configuration
func DefaultConfig() HistogramConfig {
	return HistogramConfig{
		NumBins:      30,
		BinMethod:    "auto",
		OutputDir:    ".",
		OutputPrefix: "histogram",
	}
}

// NewStreamingHistogram creates a new streaming histogram processor
func NewStreamingHistogram(config HistogramConfig) *StreamingHistogram {
	return &StreamingHistogram{
		config: config,
		dataSummary: DataSummary{
			Min: math.MaxFloat64,
			Max: -math.MaxFloat64,
		},
		dataPoints: []float64{},
	}
}

// AddValue adds a single value to the histogram
func (sh *StreamingHistogram) AddValue(value float64) {
	sh.updateSummary(value)
	sh.dataPoints = append(sh.dataPoints, value)

	if !sh.initialized && sh.dataSummary.Count > 0 {
		sh.initializeBins()
	}

	if sh.initialized {
		binIndex := sh.findBinIndex(value)
		if binIndex >= 0 && binIndex < len(sh.bins) {
			sh.bins[binIndex].Count++
		}
	}
}

// AddBatch adds a batch of values to the histogram
func (sh *StreamingHistogram) AddBatch(values []float64) {
	for _, value := range values {
		sh.AddValue(value)
	}
}

// Finalize completes the histogram computation and calculates final statistics
func (sh *StreamingHistogram) Finalize() *HistogramResult {
	if !sh.initialized && sh.dataSummary.Count > 0 {
		sh.initializeBins()
	}

	sh.calculateFinalStatistics()

	return &HistogramResult{
		Config:      sh.config,
		Bins:        sh.bins,
		TotalCount:  sh.dataSummary.Count,
		MinValue:    sh.dataSummary.Min,
		MaxValue:    sh.dataSummary.Max,
		Mean:        sh.dataSummary.Mean,
		StdDev:      math.Sqrt(sh.dataSummary.Variance),
		DataSummary: sh.dataSummary,
	}
}

// ProcessStream processes a stream of float64 values
func (sh *StreamingHistogram) ProcessStream(stream <-chan float64) *HistogramResult {
	for value := range stream {
		sh.AddValue(value)
	}
	return sh.Finalize()
}

// ProcessReader processes data from an io.Reader (one value per line)
func (sh *StreamingHistogram) ProcessReader(reader io.Reader) (*HistogramResult, error) {
	buf := make([]byte, 4096)
	var leftover []byte

	for {
		n, err := reader.Read(buf)
		if err != nil && err != io.EOF {
			return nil, err
		}

		if n == 0 {
			break
		}

		data := append(leftover, buf[:n]...)
		lines := strings.Split(string(data), "\n")

		// Process all complete lines except the last one
		for i := 0; i < len(lines)-1; i++ {
			if lines[i] == "" {
				continue
			}
			value, err := strconv.ParseFloat(strings.TrimSpace(lines[i]), 64)
			if err == nil {
				sh.AddValue(value)
			}
		}

		// Save the last incomplete line for next iteration
		leftover = []byte(lines[len(lines)-1])

		if err == io.EOF {
			break
		}
	}

	// Process any remaining data
	if len(leftover) > 0 {
		value, err := strconv.ParseFloat(strings.TrimSpace(string(leftover)), 64)
		if err == nil {
			sh.AddValue(value)
		}
	}

	return sh.Finalize(), nil
}

// SaveAsJSON saves the histogram result as JSON
func (hr *HistogramResult) SaveAsJSON(filename string) error {
	if !filepath.IsAbs(filename) {
		filename = filepath.Join(hr.Config.OutputDir, filename)
	}

	data, err := json.MarshalIndent(hr, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(filename, data, 0644)
}

// SaveAsHTML saves the histogram as HTML using ECharts
func (hr *HistogramResult) SaveAsHTML(filename string) error {
	if !filepath.IsAbs(filename) {
		filename = filepath.Join(hr.Config.OutputDir, filename)
	}

	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// Create histogram using ECharts
	histogram := charts.NewBar()
	histogram.SetGlobalOptions(
		charts.WithTitleOpts(opts.Title{
			Title:    fmt.Sprintf("Histogram (n=%d)", hr.TotalCount),
			Subtitle: fmt.Sprintf("Mean: %.2f, StdDev: %.2f", hr.Mean, hr.StdDev),
		}),
		charts.WithXAxisOpts(opts.XAxis{
			Name: "Value",
			AxisLabel: &opts.AxisLabel{
				Rotate: 45,
			},
		}),
		charts.WithYAxisOpts(opts.YAxis{
			Name: "Frequency",
		}),
		charts.WithTooltipOpts(opts.Tooltip{
			Show:      opts.Bool(true),
			Trigger:   "axis",
			TriggerOn: "mousemove",
		}),
		charts.WithInitializationOpts(opts.Initialization{
			Width:  "1200px",
			Height: "800px",
		}),
		charts.WithToolboxOpts(opts.Toolbox{
			Show: opts.Bool(true),
			Feature: &opts.ToolBoxFeature{
				SaveAsImage: &opts.ToolBoxFeatureSaveAsImage{
					Show: opts.Bool(true),
					Type: "png",
				},
				DataView: &opts.ToolBoxFeatureDataView{
					Show: opts.Bool(true),
				},
				Restore: &opts.ToolBoxFeatureRestore{
					Show: opts.Bool(true),
				},
			},
		}),
	)

	// Prepare data for ECharts
	xAxisData := make([]string, len(hr.Bins))
	barData := make([]opts.BarData, len(hr.Bins))

	for i, bin := range hr.Bins {
		xAxisData[i] = fmt.Sprintf("[%.2f, %.2f)", bin.LowerBound, bin.UpperBound)
		barData[i] = opts.BarData{Value: bin.Count}
	}

	histogram.SetXAxis(xAxisData).
		AddSeries("Frequency", barData).
		SetSeriesOptions(
			charts.WithBarChartOpts(opts.BarChart{
				BarGap: "0%",
			}),
			charts.WithLabelOpts(opts.Label{
				Show:     opts.Bool(true),
				Position: "top",
			}),
		)

	return histogram.Render(file)
}

// GenerateAllFormats generates JSON, HTML, and provides PNG conversion info
func (hr *HistogramResult) GenerateAllFormats() error {
	prefix := hr.Config.OutputPrefix
	if err := hr.SaveAsJSON(prefix + ".json"); err != nil {
		return err
	}
	if err := hr.SaveAsHTML(prefix + ".html"); err != nil {
		return err
	}
	fmt.Printf("To generate PNG, open %s.html in a browser and use the download button,\n", prefix)
	fmt.Printf("or use headless browser tools to convert HTML to PNG.\n")
	return nil
}

// Private helper methods

func (sh *StreamingHistogram) updateSummary(value float64) {
	sh.dataSummary.Count++
	sh.dataSummary.Sum += value

	if value < sh.dataSummary.Min {
		sh.dataSummary.Min = value
	}
	if value > sh.dataSummary.Max {
		sh.dataSummary.Max = value
	}

	// Online mean and variance calculation using Welford's algorithm
	if sh.dataSummary.Count == 1 {
		sh.dataSummary.Mean = value
		sh.dataSummary.Variance = 0
	} else {
		oldMean := sh.dataSummary.Mean
		sh.dataSummary.Mean = oldMean + (value-oldMean)/float64(sh.dataSummary.Count)
		sh.dataSummary.Variance = sh.dataSummary.Variance + (value-oldMean)*(value-sh.dataSummary.Mean)
	}
}

func (sh *StreamingHistogram) initializeBins() {
	if sh.dataSummary.Count == 0 {
		return
	}

	minVal := sh.config.MinValue
	maxVal := sh.config.MaxValue
	if minVal == 0 && maxVal == 0 {
		minVal = sh.dataSummary.Min
		maxVal = sh.dataSummary.Max
	}

	numBins := sh.calculateNumBins()
	binWidth := (maxVal - minVal) / float64(numBins)

	if binWidth == 0 {
		binWidth = 1.0
		maxVal = minVal + float64(numBins)
	}

	sh.binEdges = make([]float64, numBins+1)
	sh.bins = make([]Bin, numBins)

	for i := 0; i <= numBins; i++ {
		sh.binEdges[i] = minVal + float64(i)*binWidth
	}

	for i := 0; i < numBins; i++ {
		sh.bins[i] = Bin{
			LowerBound: sh.binEdges[i],
			UpperBound: sh.binEdges[i+1],
			Count:      0,
		}
	}

	sh.initialized = true
}

func (sh *StreamingHistogram) calculateNumBins() int {
	switch sh.config.BinMethod {
	case "sturges":
		return int(math.Ceil(math.Log2(float64(sh.dataSummary.Count)))) + 1
	case "sqrt":
		return int(math.Ceil(math.Sqrt(float64(sh.dataSummary.Count))))
	case "fd": // Freedman-Diaconis
		iqr := sh.calculateIQR()
		if iqr == 0 {
			return sh.config.NumBins
		}
		binWidth := 2 * iqr / math.Pow(float64(sh.dataSummary.Count), 1.0/3.0)
		if binWidth == 0 {
			return sh.config.NumBins
		}
		return int(math.Ceil((sh.dataSummary.Max - sh.dataSummary.Min) / binWidth))
	case "fixed":
		if sh.config.BinWidth > 0 {
			return int(math.Ceil((sh.dataSummary.Max - sh.dataSummary.Min) / sh.config.BinWidth))
		}
		fallthrough
	default: // "auto" or invalid
		return sh.config.NumBins
	}
}

func (sh *StreamingHistogram) calculateIQR() float64 {
	// For streaming data, we need to store all values to calculate IQR
	// This is a simplified version - in practice, you might want to use
	// approximate methods for large datasets
	return (sh.dataSummary.Max - sh.dataSummary.Min) / 4
}

func (sh *StreamingHistogram) findBinIndex(value float64) int {
	if len(sh.binEdges) == 0 {
		return -1
	}

	// Binary search for the correct bin
	left, right := 0, len(sh.binEdges)-1
	for left <= right {
		mid := left + (right-left)/2
		if sh.binEdges[mid] <= value {
			if mid == len(sh.binEdges)-1 || value < sh.binEdges[mid+1] {
				return mid
			}
			left = mid + 1
		} else {
			right = mid - 1
		}
	}

	return -1
}

func (sh *StreamingHistogram) calculateFinalStatistics() {
	if sh.dataSummary.Count > 1 {
		sh.dataSummary.Variance /= float64(sh.dataSummary.Count - 1)
	} else {
		sh.dataSummary.Variance = 0
	}

	// Calculate higher moments would require storing all data points
	// For streaming, these are set to 0 as placeholders
	sh.dataSummary.Skewness = 0
	sh.dataSummary.Kurtosis = 0
}
