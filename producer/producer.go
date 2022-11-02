// Author: Fatma Reyyan SARIKAYA

package main

import (
	"context"
	"encoding/json"
	"fmt"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/segmentio/kafka-go"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"
)

func main() {
	metric := getPrometheusMetrics(8080)

	jsonValue := ParseMetrics(metric)
	byteFormatJson, _ := json.Marshal(jsonValue)
	conn, _ := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", "hb-project", 0)
	conn.SetWriteDeadline(time.Now().Add(time.Second * 10))
	conn.WriteMessages(kafka.Message{Value: byteFormatJson})
}

func ParseMetrics(metric string) any {

	parserMetric := &expfmt.TextParser{}
	families, err := parserMetric.TextToMetricFamilies(strings.NewReader(metric))
	if err != nil {
		return fmt.Errorf("failed to Parse Input: %w", err)
	}
	outputJson := make(map[string][]map[string]map[string]any)

	for key, value := range families {
		family := outputJson[key]

		for _, m := range value.GetMetric() {
			metric := make(map[string]any)
			for _, label := range m.GetLabel() {
				metric[label.GetName()] = label.GetValue()
			}
			switch value.GetType() {
			case dto.MetricType_COUNTER:
				metric["value"] = m.GetCounter().GetValue()
			case dto.MetricType_GAUGE:
				metric["value"] = m.GetGauge().GetValue()
			default:
				return fmt.Errorf("unsupported type: %v", value.GetType())
			}
			family = append(family, map[string]map[string]any{
				value.GetName(): metric,
			})
		}

		outputJson[key] = family
	}

	return outputJson

}

func getPrometheusMetrics(serverPort int) string {
	requestURL := fmt.Sprintf("http://localhost:%d/metrics", serverPort)
	req, err := http.NewRequest(http.MethodGet, requestURL, nil)
	if err != nil {
		fmt.Printf("client: could not create request: %s\n", err)
		os.Exit(1)
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Printf("client: error making http request: %s\n", err)
		os.Exit(1)
	}

	resBody, err := ioutil.ReadAll(res.Body)
	if err != nil {
		fmt.Printf("client: could not read response body: %s\n", err)
		os.Exit(1)
	}
	return string(resBody)
}
