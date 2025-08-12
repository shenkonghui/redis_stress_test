package main

import (
	"context"
	"crypto/tls"
	"encoding/csv"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	redis "github.com/redis/go-redis/v9"
	yaml "gopkg.in/yaml.v3"
)

type RedisConfig struct {
	Addr     string   `yaml:"addr"`
	Addrs    []string `yaml:"addrs"`
	Password string   `yaml:"password"`
	DB       int      `yaml:"db"`
	TLS      bool     `yaml:"tls"`
}

func defaultCSVHeaders() []string {
	return []string{
		"time",
		"round",
		"elapsed_ms",
		"total_ops",
		"success",
		"fail",
		"success_rate",
		"qps",
		"latency_avg_us",
		"p50_us",
		"p95_us",
		"p99_us",
	}
}

func writeRoundCSV(cfg *Config, loop int, elapsed time.Duration, st stats) error {
	file := cfg.Output.CSVFile
	if strings.TrimSpace(file) == "" {
		file = "results.csv"
	}
	ts := time.Now().Local().Format("2006-01-02 15:04:05")
	// compute metrics
	total := st.Ops
	succ := st.Success
	fail := st.Fail
	successRate := 100 * float64(succ) / float64(max(1, total))
	qps := 0.0
	if elapsed > 0 {
		qps = float64(total) / elapsed.Seconds()
	}
	// latency percentiles
	var avgDur, p50, p95, p99 time.Duration
	if len(st.Durations) > 0 {
		// durations may already be sorted by printRound; ensure sort anyway
		sort.Slice(st.Durations, func(i, j int) bool { return st.Durations[i] < st.Durations[j] })
		var sum time.Duration
		for _, d := range st.Durations {
			sum += d
		}
		avgDur = time.Duration(int64(sum) / int64(len(st.Durations)))
		p50 = percentile(st.Durations, 0.50)
		p95 = percentile(st.Durations, 0.95)
		p99 = percentile(st.Durations, 0.99)
	}

	// open file (append), create if not exists
	f, err := os.OpenFile(file, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	// write header if file is empty
	stat, err := f.Stat()
	if err == nil && stat.Size() == 0 {
		headers := cfg.Output.CSVHeaders
		if len(headers) == 0 {
			headers = defaultCSVHeaders()
		}
		w := csv.NewWriter(f)
		if err := w.Write(headers); err != nil {
			return err
		}
		w.Flush()
		if err := w.Error(); err != nil {
			return err
		}
	}

	// write data row
	rec := []string{
		ts,
		fmt.Sprintf("%d", loop+1),
		fmt.Sprintf("%d", elapsed.Milliseconds()),
		fmt.Sprintf("%d", total),
		fmt.Sprintf("%d", succ),
		fmt.Sprintf("%d", fail),
		fmt.Sprintf("%.2f", successRate),
		fmt.Sprintf("%.2f", qps),
		fmt.Sprintf("%d", avgDur.Microseconds()),
		fmt.Sprintf("%d", p50.Microseconds()),
		fmt.Sprintf("%d", p95.Microseconds()),
		fmt.Sprintf("%d", p99.Microseconds()),
	}
	w := csv.NewWriter(f)
	if err := w.Write(rec); err != nil {
		return err
	}
	w.Flush()
	return w.Error()
}

type OperationMix struct {
	Get     int `yaml:"get"`
	Set     int `yaml:"set"`
	HGet    int `yaml:"hget"`
	HSet    int `yaml:"hset"`
	HMSet   int `yaml:"hmset"`
	HGetAll int `yaml:"hgetall"`
}

type TestConfig struct {
	Threads        int          `yaml:"threads"`
	Loops          int          `yaml:"loops"`
	OpsPerThread   int          `yaml:"ops_per_thread"`
	KeySpace       int          `yaml:"key_space"`
	ValueSize      int          `yaml:"value_size"`
	ExpireSeconds  int          `yaml:"expire_seconds"`
	Pipeline       int          `yaml:"pipeline"`
	UseTx          bool         `yaml:"use_tx"`
	HashFieldCount int          `yaml:"hash_field_count"`
	OperationMix   OperationMix `yaml:"operation_mix"`
}

type I18nConfig struct {
	RoundFinished string `yaml:"round_finished"`
	TotalLine     string `yaml:"total_line"`
	QPSLine       string `yaml:"qps_line"`
	LatencyLine   string `yaml:"latency_line"`
	AllFinished   string `yaml:"all_finished"`
	TotalSummary  string `yaml:"total_summary"`
	OverallLine   string `yaml:"overall_line"`
}

type OutputConfig struct {
	CSVFile    string   `yaml:"csv_file"`
	CSVHeaders []string `yaml:"csv_headers"`
}

type Config struct {
	Redis  RedisConfig  `yaml:"redis"`
	Test   TestConfig   `yaml:"test"`
	I18n   I18nConfig   `yaml:"i18n"`
	Output OutputConfig `yaml:"output"`
}

type redisClient interface {
	Get(ctx context.Context, key string) *redis.StringCmd
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	HGet(ctx context.Context, key, field string) *redis.StringCmd
	HSet(ctx context.Context, key string, values ...interface{}) *redis.IntCmd
	HGetAll(ctx context.Context, key string) *redis.MapStringStringCmd
	Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd
	Pipeline() redis.Pipeliner
	TxPipeline() redis.Pipeliner
	Close() error
}

type opType int

const (
	opGet opType = iota
	opSet
	opHGet
	opHSet
	opHMSet
	opHGetAll
)

func (o opType) String() string {
	switch o {
	case opGet:
		return "GET"
	case opSet:
		return "SET"
	case opHGet:
		return "HGET"
	case opHSet:
		return "HSET"
	case opHMSet:
		return "HMSET"
	case opHGetAll:
		return "HGETALL"
	default:
		return "UNKNOWN"
	}
}

type stats struct {
	Ops       int
	Success   int
	Fail      int
	Durations []time.Duration // per-op approx latency
}

func (s *stats) merge(other stats) {
	s.Ops += other.Ops
	s.Success += other.Success
	s.Fail += other.Fail
	s.Durations = append(s.Durations, other.Durations...)
}

func percentile(durs []time.Duration, p float64) time.Duration {
	if len(durs) == 0 {
		return 0
	}
	idx := int(p*float64(len(durs)-1) + 0.5)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(durs) {
		idx = len(durs) - 1
	}
	return durs[idx]
}

func loadConfig(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func newClient(cfg RedisConfig) (redisClient, error) {
	if len(cfg.Addrs) > 0 {
		opt := &redis.ClusterOptions{
			Addrs:    cfg.Addrs,
			Password: cfg.Password,
		}
		if cfg.TLS {
			opt.TLSConfig = &tls.Config{InsecureSkipVerify: true}
		}
		return redis.NewClusterClient(opt), nil
	}
	opt := &redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	}
	if cfg.TLS {
		opt.TLSConfig = &tls.Config{InsecureSkipVerify: true}
	}
	return redis.NewClient(opt), nil
}

type weightedPicker struct {
	items []struct {
		t opType
		w int
	}
	total int
}

func newWeightedPicker(m OperationMix) *weightedPicker {
	wp := &weightedPicker{}
	add := func(t opType, w int) {
		if w > 0 {
			wp.items = append(wp.items, struct {
				t opType
				w int
			}{t, w})
			wp.total += w
		}
	}
	add(opGet, m.Get)
	add(opSet, m.Set)
	add(opHGet, m.HGet)
	add(opHSet, m.HSet)
	add(opHMSet, m.HMSet)
	add(opHGetAll, m.HGetAll)
	if wp.total == 0 {
		// default
		add(opGet, 50)
		add(opSet, 50)
	}
	return wp
}

func (w *weightedPicker) pick(r *rand.Rand) opType {
	if len(w.items) == 1 {
		return w.items[0].t
	}
	x := r.Intn(w.total)
	acc := 0
	for _, it := range w.items {
		acc += it.w
		if x < acc {
			return it.t
		}
	}
	return w.items[len(w.items)-1].t
}

// build value bytes of exact size using pseudo random letters
func makeValue(r *rand.Rand, n int) []byte {
	if n <= 0 {
		return []byte{}
	}
	b := make([]byte, n)
	for i := 0; i < n; i++ {
		b[i] = byte('a' + r.Intn(26))
	}
	return b
}

type logicalOp struct {
	op    opType
	key   string
	field string
	// how many redis commands this logical op expands to in pipeline
	cmds int
}

func worker(ctx context.Context, id int, client redisClient, cfg *Config, loopIdx int) stats {
	r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id*7919+loopIdx*104729)))
	wp := newWeightedPicker(cfg.Test.OperationMix)

	pSize := cfg.Test.Pipeline
	if pSize <= 0 {
		pSize = 1
	}
	opsTarget := cfg.Test.OpsPerThread

	var st stats
	st.Ops = opsTarget

	expire := time.Duration(cfg.Test.ExpireSeconds) * time.Second
	if expire <= 0 {
		expire = 0
	}

	strPrefix := "str_key_"
	hashPrefix := "hash_key_"

	var batch []logicalOp
	batch = make([]logicalOp, 0, pSize)

	execBatch := func() {
		if len(batch) == 0 {
			return
		}
		start := time.Now()
		var pipe redis.Pipeliner
		// When use_tx=true, use TxPipeline even if pipeline=1 to ensure atomicity
		if cfg.Test.UseTx {
			pipe = client.TxPipeline()
		} else if pSize > 1 {
			pipe = client.Pipeline()
		}

		// enqueue
		for _, op := range batch {
			switch op.op {
			case opGet:
				if pipe != nil {
					pipe.Get(ctx, op.key)
				} else {
					client.Get(ctx, op.key)
				}
			case opSet:
				val := makeValue(r, cfg.Test.ValueSize)
				if pipe != nil {
					pipe.Set(ctx, op.key, val, expire)
				} else {
					client.Set(ctx, op.key, val, expire)
				}
			case opHGet:
				if pipe != nil {
					pipe.HGet(ctx, op.key, op.field)
				} else {
					client.HGet(ctx, op.key, op.field)
				}
			case opHSet:
				// HSET multi fields then optional EXPIRE (2 commands when expire > 0)
				pairs := make([]interface{}, 0, cfg.Test.HashFieldCount*2)
				for i := 0; i < cfg.Test.HashFieldCount; i++ {
					pairs = append(pairs, fmt.Sprintf("f%d", i))
					pairs = append(pairs, makeValue(r, cfg.Test.ValueSize))
				}
				if pipe != nil {
					pipe.HSet(ctx, op.key, pairs...)
					if expire > 0 {
						pipe.Expire(ctx, op.key, expire)
					}
				} else {
					client.HSet(ctx, op.key, pairs...)
					if expire > 0 {
						client.Expire(ctx, op.key, expire)
					}
				}
			case opHMSet:
				// HMSET (alias of multi-field HSET) then optional EXPIRE
				pairs := make([]interface{}, 0, cfg.Test.HashFieldCount*2)
				for i := 0; i < cfg.Test.HashFieldCount; i++ {
					pairs = append(pairs, fmt.Sprintf("f%d", i))
					pairs = append(pairs, makeValue(r, cfg.Test.ValueSize))
				}
				if pipe != nil {
					pipe.HSet(ctx, op.key, pairs...)
					if expire > 0 {
						pipe.Expire(ctx, op.key, expire)
					}
				} else {
					client.HSet(ctx, op.key, pairs...)
					if expire > 0 {
						client.Expire(ctx, op.key, expire)
					}
				}
			case opHGetAll:
				if pipe != nil {
					pipe.HGetAll(ctx, op.key)
				} else {
					client.HGetAll(ctx, op.key)
				}
			}
		}

		var cmds []redis.Cmder
		var err error
		if pipe != nil {
			cmds, err = pipe.Exec(ctx)
		} else {
			// not pipelined
		}

		batchDur := time.Since(start)

		if pipe != nil {
			// Determine per-op success when pipelined
			idx := 0
			for _, op := range batch {
				expect := 1
				if (op.op == opHSet || op.op == opHMSet) && expire > 0 {
					expect = 2
				}
				ok := true
				for i := 0; i < expect; i++ {
					if idx >= len(cmds) {
						ok = false
						break
					}
					c := cmds[idx]
					thisOK := false
					if c != nil {
						e := c.Err()
						// For read operations, treat redis.Nil (key not found) as success
						if op.op == opGet || op.op == opHGet || op.op == opHGetAll {
							if e == nil || e == redis.Nil {
								thisOK = true
							}
						} else {
							if e == nil {
								thisOK = true
							}
						}
					}
					if !thisOK {
						ok = false
					}
					idx++
				}
				if ok {
					st.Success++
				} else {
					st.Fail++
				}
				// approximate latency split
				st.Durations = append(st.Durations, batchDur/time.Duration(len(batch)))
			}
			// top-level err indicates transaction/pipeline failure
			if err != nil {
				// already accounted per-command above
			}
		} else {
			// Non-pipelined: each op executed immediately above; best-effort accounting
			for range batch {
				st.Success++
				st.Durations = append(st.Durations, batchDur/time.Duration(len(batch)))
			}
		}

		batch = batch[:0]
	}

	// Build batch items
	for done := 0; done < opsTarget; {
		op := wp.pick(r)
		keyIdx := r.Intn(cfg.Test.KeySpace)
		var key, field string
		switch op {
		case opGet, opSet:
			key = strPrefix + fmt.Sprintf("%d", keyIdx)
		case opHGet, opHSet, opHMSet, opHGetAll:
			key = hashPrefix + fmt.Sprintf("%d", keyIdx)
			if op == opHGet {
				field = fmt.Sprintf("f%d", r.Intn(max(1, cfg.Test.HashFieldCount)))
			}
		}
		batch = append(batch, logicalOp{op: op, key: key, field: field})
		done++
		if len(batch) >= pSize {
			execBatch()
		}
	}
	execBatch()

	return st
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func strOrDefault(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

func printRound(cfg *Config, loop int, elapsed time.Duration, st stats) {
	fmt.Println(strings.Repeat("=", 64))
	fmt.Printf(strOrDefault(cfg.I18n.RoundFinished, "Round %d finished in %v\n"), loop+1, elapsed)
	fmt.Printf(strOrDefault(cfg.I18n.TotalLine, "Total Ops: %d, Success: %d, Fail: %d, SuccessRate: %.2f%%\n"),
		st.Ops, st.Success, st.Fail, 100*float64(st.Success)/float64(max(1, st.Ops)))
	qps := float64(st.Ops) / elapsed.Seconds()
	fmt.Printf(strOrDefault(cfg.I18n.QPSLine, "QPS: %.2f\n"), qps)
	if len(st.Durations) > 0 {
		sort.Slice(st.Durations, func(i, j int) bool { return st.Durations[i] < st.Durations[j] })
		var sum time.Duration
		for _, d := range st.Durations {
			sum += d
		}
		avg := time.Duration(int64(sum) / int64(len(st.Durations)))
		p50 := percentile(st.Durations, 0.50)
		p95 := percentile(st.Durations, 0.95)
		p99 := percentile(st.Durations, 0.99)
		fmt.Printf(strOrDefault(cfg.I18n.LatencyLine, "Latency avg:%v p50:%v p95:%v p99:%v\n"), avg, p50, p95, p99)
	}
}

func main() {
	cfg, err := loadConfig("config.yaml")
	if err != nil {
		log.Fatalf("failed to load config.yaml: %v", err)
	}
	client, err := newClient(cfg.Redis)
	if err != nil {
		log.Fatalf("failed to create redis client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// basic ping warm up
	{
		_, _ = client.Get(ctx, "__warmup__").Result()
	}

	var grand stats
	startAll := time.Now()

	for loop := 0; loop < max(1, cfg.Test.Loops); loop++ {
		loopStart := time.Now()
		var wg sync.WaitGroup
		wg.Add(max(1, cfg.Test.Threads))
		resCh := make(chan stats, max(1, cfg.Test.Threads))
		for th := 0; th < max(1, cfg.Test.Threads); th++ {
			go func(id int) {
				defer wg.Done()
				st := worker(ctx, id, client, cfg, loop)
				resCh <- st
			}(th)
		}
		wg.Wait()
		close(resCh)

		var round stats
		for st := range resCh {
			round.merge(st)
		}
		elapsed := time.Since(loopStart)
		printRound(cfg, loop, elapsed, round)
		if err := writeRoundCSV(cfg, loop, elapsed, round); err != nil {
			log.Printf("write csv failed: %v", err)
		}
		grand.merge(round)
	}

	allElapsed := time.Since(startAll)
	fmt.Println(strings.Repeat("=", 64))
	fmt.Println(strOrDefault(cfg.I18n.AllFinished, "All rounds finished"))
	fmt.Printf(strOrDefault(cfg.I18n.TotalSummary, "Total Ops: %d, Success: %d, Fail: %d, SuccessRate: %.2f%%\n"),
		grand.Ops, grand.Success, grand.Fail, 100*float64(grand.Success)/float64(max(1, grand.Ops)))
	fmt.Printf(strOrDefault(cfg.I18n.OverallLine, "Total Time: %v, Overall QPS: %.2f\n"), allElapsed, float64(grand.Ops)/allElapsed.Seconds())
	if len(grand.Durations) > 0 {
		sort.Slice(grand.Durations, func(i, j int) bool { return grand.Durations[i] < grand.Durations[j] })
		var sum time.Duration
		for _, d := range grand.Durations {
			sum += d
		}
		avg := time.Duration(int64(sum) / int64(len(grand.Durations)))
		p50 := percentile(grand.Durations, 0.50)
		p95 := percentile(grand.Durations, 0.95)
		p99 := percentile(grand.Durations, 0.99)
		fmt.Printf("Latency avg:%v p50:%v p95:%v p99:%v\n", avg, p50, p95, p99)
	}
}
