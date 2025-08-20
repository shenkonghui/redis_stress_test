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
		"slow_ops",
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
		fmt.Sprintf("%d", st.SlowOps),
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
	LPush   int `yaml:"lpush"`
	LPop    int `yaml:"lpop"`
	LLen    int `yaml:"llen"`
}

type TestConfig struct {
	Threads         int          `yaml:"threads"`
	Loops           int          `yaml:"loops"`
	OpsPerThread    int          `yaml:"ops_per_thread"`
	KeySpace        int          `yaml:"key_space"`
	StringValueSize int          `yaml:"string_value_size"`
	HashValueSize   int          `yaml:"hash_value_size"`
	ListValueSize   int          `yaml:"list_value_size"`
	ExpireSeconds   int          `yaml:"expire_seconds"`
	Pipeline        int          `yaml:"pipeline"`
	UseTx           bool         `yaml:"use_tx"`
	HashFieldCount  int          `yaml:"hash_field_count"`
	SlowThresholdMs int          `yaml:"slow_threshold_ms"`
	OperationMix    OperationMix `yaml:"operation_mix"`
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
	CSVFile        string   `yaml:"csv_file"`
	CSVHeaders     []string `yaml:"csv_headers"`
	VerboseLogging bool     `yaml:"verbose_logging"`
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
	LPush(ctx context.Context, key string, values ...interface{}) *redis.IntCmd
	LPop(ctx context.Context, key string) *redis.StringCmd
	LLen(ctx context.Context, key string) *redis.IntCmd
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
	opLPush
	opLPop
	opLLen
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
	case opLPush:
		return "LPUSH"
	case opLPop:
		return "LPOP"
	case opLLen:
		return "LLEN"
	default:
		return "UNKNOWN"
	}
}

type stats struct {
	Ops       int
	Success   int
	Fail      int
	SlowOps   int             // 慢操作次数
	Durations []time.Duration // per-op approx latency
	// 每种命令类型的执行次数统计
	CmdCounts map[opType]int
}

func (s *stats) merge(other stats) {
	s.Ops += other.Ops
	s.Success += other.Success
	s.Fail += other.Fail
	s.SlowOps += other.SlowOps
	s.Durations = append(s.Durations, other.Durations...)
	// 合并命令计数统计
	if s.CmdCounts == nil {
		s.CmdCounts = make(map[opType]int)
	}
	for cmd, count := range other.CmdCounts {
		s.CmdCounts[cmd] += count
	}
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
	add(opLPush, m.LPush)
	add(opLPop, m.LPop)
	add(opLLen, m.LLen)
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
	st.CmdCounts = make(map[opType]int)
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

		// collect commands when not using pipeline, so we can evaluate errors uniformly
		cmdsLocal := make([]redis.Cmder, 0, len(batch)*2)

		// enqueue
		for _, op := range batch {
			switch op.op {
			case opGet:
				if pipe != nil {
					pipe.Get(ctx, op.key)
				} else {
					cmd := client.Get(ctx, op.key)
					cmdsLocal = append(cmdsLocal, cmd)
				}
			case opSet:
				val := makeValue(r, cfg.Test.StringValueSize)
				if pipe != nil {
					pipe.Set(ctx, op.key, val, expire)
				} else {
					cmd := client.Set(ctx, op.key, val, expire)
					cmdsLocal = append(cmdsLocal, cmd)
				}
			case opHGet:
				if pipe != nil {
					pipe.HGet(ctx, op.key, op.field)
				} else {
					cmd := client.HGet(ctx, op.key, op.field)
					cmdsLocal = append(cmdsLocal, cmd)
				}
			case opHSet:
				// HSET multi fields then optional EXPIRE (2 commands when expire > 0)
				pairs := make([]interface{}, 0, cfg.Test.HashFieldCount*2)
				for i := 0; i < cfg.Test.HashFieldCount; i++ {
					pairs = append(pairs, fmt.Sprintf("f%d", i))
					pairs = append(pairs, makeValue(r, cfg.Test.HashValueSize))
				}
				if pipe != nil {
					pipe.HSet(ctx, op.key, pairs...)
					if expire > 0 {
						pipe.Expire(ctx, op.key, expire)
					}
				} else {
					cmd := client.HSet(ctx, op.key, pairs...)
					cmdsLocal = append(cmdsLocal, cmd)
					if expire > 0 {
						cmd2 := client.Expire(ctx, op.key, expire)
						cmdsLocal = append(cmdsLocal, cmd2)
					}
				}
			case opHMSet:
				// HMSET (alias of multi-field HSET) then optional EXPIRE
				pairs := make([]interface{}, 0, cfg.Test.HashFieldCount*2)
				for i := 0; i < cfg.Test.HashFieldCount; i++ {
					pairs = append(pairs, fmt.Sprintf("f%d", i))
					pairs = append(pairs, makeValue(r, cfg.Test.HashValueSize))
				}
				if pipe != nil {
					pipe.HSet(ctx, op.key, pairs...)
					if expire > 0 {
						pipe.Expire(ctx, op.key, expire)
					}
				} else {
					cmd := client.HSet(ctx, op.key, pairs...)
					cmdsLocal = append(cmdsLocal, cmd)
					if expire > 0 {
						cmd2 := client.Expire(ctx, op.key, expire)
						cmdsLocal = append(cmdsLocal, cmd2)
					}
				}
			case opHGetAll:
				if pipe != nil {
					pipe.HGetAll(ctx, op.key)
				} else {
					cmd := client.HGetAll(ctx, op.key)
					cmdsLocal = append(cmdsLocal, cmd)
				}
			case opLPush:
				val := makeValue(r, cfg.Test.ListValueSize)
				if pipe != nil {
					pipe.LPush(ctx, op.key, val)
					if expire > 0 {
						pipe.Expire(ctx, op.key, expire)
					}
				} else {
					cmd := client.LPush(ctx, op.key, val)
					cmdsLocal = append(cmdsLocal, cmd)
					if expire > 0 {
						cmd2 := client.Expire(ctx, op.key, expire)
						cmdsLocal = append(cmdsLocal, cmd2)
					}
				}
			case opLPop:
				if pipe != nil {
					pipe.LPop(ctx, op.key)
				} else {
					cmd := client.LPop(ctx, op.key)
					cmdsLocal = append(cmdsLocal, cmd)
				}
			case opLLen:
				if pipe != nil {
					pipe.LLen(ctx, op.key)
				} else {
					cmd := client.LLen(ctx, op.key)
					cmdsLocal = append(cmdsLocal, cmd)
				}
			}
		}

		var cmds []redis.Cmder
		var err error
		if pipe != nil {
			cmds, err = pipe.Exec(ctx)
		} else {
			cmds = cmdsLocal
		}

		batchDur := time.Since(start)

		// 检测慢操作
		slowThreshold := time.Duration(cfg.Test.SlowThresholdMs) * time.Millisecond
		perOpDur := batchDur / time.Duration(len(batch))
		isSlowBatch := batchDur >= slowThreshold

		// Determine per-op success and print failures
		idx := 0
		for _, op := range batch {
			expect := 1
			if (op.op == opHSet || op.op == opHMSet || op.op == opLPush) && expire > 0 {
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
				var e error
				if c != nil {
					e = c.Err()
					// For read operations, treat redis.Nil (key not found) as success
					if op.op == opGet || op.op == opHGet || op.op == opHGetAll || op.op == opLPop || op.op == opLLen {
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
					// log failure details
					sub := ""
					switch op.op {
					case opGet:
						sub = "GET"
					case opSet:
						sub = "SET"
					case opHGet:
						sub = "HGET"
					case opHSet:
						if i == 0 {
							sub = "HSET"
						} else {
							sub = "EXPIRE"
						}
					case opHMSet:
						if i == 0 {
							sub = "HMSET"
						} else {
							sub = "EXPIRE"
						}
					case opHGetAll:
						sub = "HGETALL"
					case opLPush:
						if i == 0 {
							sub = "LPUSH"
						} else {
							sub = "EXPIRE"
						}
					case opLPop:
						sub = "LPOP"
					case opLLen:
						sub = "LLEN"
					}
					if cfg.Output.VerboseLogging {
						log.Printf("Operation failed: op=%s key=%s field=%s sub=%s err=%v", op.op.String(), op.key, op.field, sub, e)
					}
				}
				idx++
			}
			if ok {
				st.Success++
				// 检测并统计慢操作
				if isSlowBatch {
					st.SlowOps++
					log.Printf("Slow operation detected: op=%s key=%s field=%s duration=%v (threshold=%v)",
						op.op.String(), op.key, op.field, perOpDur, slowThreshold)
				}
			} else {
				st.Fail++
				// 失败的操作也可能是慢操作
				if isSlowBatch {
					st.SlowOps++
					log.Printf("Slow operation detected (failed): op=%s key=%s field=%s duration=%v (threshold=%v)",
						op.op.String(), op.key, op.field, perOpDur, slowThreshold)
				}
			}
			// approximate latency split
			st.Durations = append(st.Durations, batchDur/time.Duration(len(batch)))
		}
		// top-level err indicates transaction/pipeline failure
		if err != nil {
			log.Printf("Pipeline execution error: %v", err)
		}

		batch = batch[:0]
	}

	// Build batch items
	ks := max(1, cfg.Test.KeySpace)
	for done := 0; done < opsTarget; {
		op := wp.pick(r)
		keyIdx := r.Intn(ks)
		var key, field string
		switch op {
		case opGet, opSet:
			key = strPrefix + fmt.Sprintf("%d", keyIdx)
		case opHGet, opHSet, opHMSet, opHGetAll:
			key = hashPrefix + fmt.Sprintf("%d", keyIdx)
			if op == opHGet {
				field = fmt.Sprintf("f%d", r.Intn(max(1, cfg.Test.HashFieldCount)))
			}
		case opLPush, opLPop, opLLen:
			key = "list:" + fmt.Sprintf("%d", keyIdx)
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

// printRound 打印单轮测试结果
func printRound(cfg *Config, loop int, elapsed time.Duration, st stats) {
	fmt.Println(strings.Repeat("=", 64))
	fmt.Printf(strOrDefault(cfg.I18n.RoundFinished, "Round %d finished in %v\n"), loop+1, elapsed)
	fmt.Printf(strOrDefault(cfg.I18n.TotalLine, "Total Ops: %d, Success: %d, Fail: %d, SuccessRate: %.2f%%\n"),
		st.Ops, st.Success, st.Fail, 100*float64(st.Success)/float64(max(1, st.Ops)))
	qps := float64(st.Ops) / elapsed.Seconds()
	fmt.Printf(strOrDefault(cfg.I18n.QPSLine, "QPS: %.2f\n"), qps)

	var avg time.Duration
	if len(st.Durations) > 0 {
		sort.Slice(st.Durations, func(i, j int) bool { return st.Durations[i] < st.Durations[j] })
		var sum time.Duration
		for _, d := range st.Durations {
			sum += d
		}
		avg = time.Duration(int64(sum) / int64(len(st.Durations)))
		p50 := percentile(st.Durations, 0.50)
		p95 := percentile(st.Durations, 0.95)
		p99 := percentile(st.Durations, 0.99)
		fmt.Printf(strOrDefault(cfg.I18n.LatencyLine, "Latency avg:%v p50:%v p95:%v p99:%v\n"), avg, p50, p95, p99)
	}

	// 打印慢操作统计
	if st.SlowOps > 0 {
		fmt.Printf("慢操作统计: %d 次操作超过 %dms 阈值\n", st.SlowOps, cfg.Test.SlowThresholdMs)
	}

	// 如果启用了详细日志，打印命令执行统计汇总
	if cfg.Output.VerboseLogging {
		fmt.Println(strings.Repeat("-", 40))
		fmt.Printf("第 %d 轮命令执行统计汇总:\n", loop+1)
		fmt.Printf("  总命令数: %d\n", st.Ops)
		fmt.Printf("  成功命令: %d\n", st.Success)
		fmt.Printf("  失败命令: %d\n", st.Fail)
		fmt.Printf("  慢操作数: %d\n", st.SlowOps)
		fmt.Printf("  成功率: %.2f%%\n", 100*float64(st.Success)/float64(max(1, st.Ops)))
		fmt.Printf("  平均延迟: %v\n", avg)
		fmt.Println(strings.Repeat("-", 40))
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
	// 打印总慢操作统计
	if grand.SlowOps > 0 {
		fmt.Printf("总慢操作统计: %d 次操作超过 %dms 阈值 (占比: %.2f%%)\n",
			grand.SlowOps, cfg.Test.SlowThresholdMs, 100*float64(grand.SlowOps)/float64(max(1, grand.Ops)))
	}
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
