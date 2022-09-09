package core

import (
	"context"
	_ "database/sql"
	_ "encoding/json"
	"sort"

	//"net"
	"time"

	//"errors"
	"fmt"
	//"github.com/boltdb/bolt"
	"github.com/go-redis/redis/v8"
	_ "reflect"
	//"sync"
)

var sillyGirl Bucket
var Zero Bucket

/*
func MakeBucket(name string) Bucket {
	if Zero == nil {
		logs.Error("找不到存储器，开发者自行实现接口。")
	}
	return Zero.Copy(name)
}
*/

type Bucket interface {
	Copy(string) Bucket
	Set(interface{}, interface{}) error
	Empty() (bool, error)
	Size() (int64, error)
	Delete() error
	Buckets() ([][]byte, error)
	GetString(...interface{}) string
	GetBytes(string) []byte
	GetInt(interface{}, ...int) int
	GetBool(interface{}, ...bool) bool
	Foreach(func([]byte, []byte) error)
	//Create(interface{}) error
	//First(interface{}) error
	String() string
}

type Redis string

type KeyValueMap struct {
	key   string
	value string
}

/*
var (
	ctx             = context.Background()
	linkRedisMethod sync.Once
	rdb             *redis.Client
)*/
var rdb *redis.Client
var ctx = context.Background()

func init() {
	rdb = redis.NewClient(&redis.Options{
		//连接信息
		Network:  "tcp",               //网络类型，tcp or unix，默认tcp
		Addr:     "121.5.76.254:6379", //主机名+冒号+端口，默认localhost:6379
		Password: "123321",            //密码
		DB:       0,                   // redis数据库index

		//连接池容量及闲置连接数量
		PoolSize:     15, // 连接池最大socket连接数，默认为4倍CPU数， 4 * runtime.NumCPU
		MinIdleConns: 10, //在启动阶段创建指定数量的Idle连接，并长期维持idle状态的连接数不少于指定数量；。

		//超时
		DialTimeout:  5 * time.Second, //连接建立超时时间，默认5秒。
		ReadTimeout:  3 * time.Second, //读超时，默认3秒， -1表示取消读超时
		WriteTimeout: 3 * time.Second, //写超时，默认等于读超时
		PoolTimeout:  4 * time.Second, //当所有连接都处在繁忙状态时，客户端等待可用连接的最大等待时长，默认为读超时+1秒。

		//闲置连接检查包括IdleTimeout，MaxConnAge
		IdleCheckFrequency: 60 * time.Second, //闲置连接检查的周期，默认为1分钟，-1表示不做周期性检查，只在客户端获取连接时对闲置连接进行处理。
		IdleTimeout:        5 * time.Minute,  //闲置超时，默认5分钟，-1表示取消闲置超时检查
		MaxConnAge:         0 * time.Second,  //连接存活时长，从创建开始计时，超过指定时长则关闭连接，默认为0，即不关闭存活时长较长的连接

		//命令执行失败时的重试策略
		MaxRetries:      1,                      // 命令执行失败时，最多重试多少次，默认为0即不重试
		MinRetryBackoff: 8 * time.Millisecond,   //每次计算重试间隔时间的下限，默认8毫秒，-1表示取消间隔
		MaxRetryBackoff: 512 * time.Millisecond, //每次计算重试间隔时间的上限，默认512毫秒，-1表示取消间隔

	})
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		//logs.Info("[Error]", "redis启动:", err.Error())
		//fmt.Println(fmt.Sprintf("[Error]：redis启动:%s", err.Error()))
	} else {
		//logs.Info("[Success]", "redis已链接")
		//fmt.Println("[Success]：redis已链接")
	}

	/*
		// 建立连接池
		RedisClient = &redis.Pool{
			// 从配置文件获取maxidle以及maxactive，取不到则用后面的默认值
			MaxIdle: 16, //最初的连接数量
			// MaxActive:1000000,    //最大连接数量
			MaxActive:   0,                 //连接池最大连接数量,不确定可以用0（0表示自动定义），按需分配
			IdleTimeout: 300 * time.Second, //连接关闭时间 300秒 （300秒不使用自动关闭）
			Dial: func() (redis.Conn, error) { //要连接的redis数据库
				c, err := redis.Dial(RedisConf["type"], RedisConf["address"])
				if err != nil {
					return nil, err
				}
				if _, err := c.Do("AUTH", RedisConf["auth"]); err != nil {
					c.Close()
					return nil, err
				}
				return c, nil
			},
		}
		/*
		//初始化日志
		linkRedisMethod.Do(func() {
			//连接数据库
			rdb = redis.NewClient(&redis.Options{
				Addr:     "121.5.76.254:6379", // 对应的ip以及端口号
				Password: "123321",            // 数据库的密码
				DB:       0,                   // 数据库的编号，默认的话是0
			})
			// 连接测活
			_, err := rdb.Ping(ctx).Result()
			if err != nil {
				panic(err)
			}
			fmt.Println("连接Redis成功")
		})

		// Output: PONG <nil>

		//core.InitMyLog()
		/*
			logs.Info("初始化sqlite3数据库")
			var err error
			db, err = sql.Open(dbDriverName, dbName)
			if err != nil {
				logs.Info("打开数据库错误", err)
			}
			//defer db.Close()
			if db == nil {
				logs.Info("sqlite3数据库初始化错误")
			} else {
				logs.Info("sqlite3连接成功")
			}
	*/
	//ExampleNewClient()
	//Zero = MakeBucket("sillyGirl")

}

func (s Redis) String() string {
	//logs.Info(string(s))
	return string(s)
}

/*
func createTable(bucketName string) error {
	logs.Info("创建表", bucketName)
	_, err := rdb.HLen(ctx, bucketName).Result()
	if err != nil {
		panic(err)
	}
		sql := `create table if not exists ` + bucketName + `(
			key text primary key,
			value text
		)`
		_, err := db.Exec(sql)
	return err
}
*/

/*
func checkTable(tableName string) (bool, error) {
	exist := false
	//sql := `SELECT count(*) as cnt FROM sqlite_master WHERE type = 'table' AND name='` + tableName + `'`
	//rows, err := db.Query(sql)
	rows, err :=rdb.HGetAll(tableName).Result()
	if err != nil {
		return false, err
	}
	for rows.Next() {
		exist = true
		break
	}
	logs.Info("检查", tableName, "是否存在：", exist)
	return exist, nil
}


func queryData(table, key string) (string, error) {
	logs.Info("查询数据库数据", `table=`, table, `key=`, key)
	//sql := `select * from ` + table + ` where key='` + key + `'`
	//logs.Info(sql)
	rows, err := rdb.HGet(ctx, table, key).Result()
	//rows, err := db.Query(sql)
	logs.Info("查询数据", rows, err)
	if err != nil {
		return "", err
	}
	//defer rows.Close()
	var result = make([]KeyValueMap, 0)
	if len(result) <= 0 {
		return "", errors.New("没有" + key + "值")
	} else {
		return result[0].value, nil
	}
}
*/

func MakeBucket(name string) Bucket {
	var store Bucket = Redis(name)
	//createTable(name)
	return store
}

/*
//生成一个自动增加的整数
func (s Redis) NextSequence() (int64, error) {
	//查询最后一条记录
	sql := `select * from ` + s.String() + ` order by key desc limit 1`
	row, err := db.Query(sql)
	if err != nil {
		return -1, err
	}
	defer row.Close()
	var key, value string
	for row.Next() {
		row.Scan(&key, &value)
	}
	if key != "" {
		if no, err := strconv.ParseInt(key, 10, 64); err != nil {
			return -1, err
		} else {
			return no + 1, nil
		}
	} else {
		return 0, nil
	}
}
*/

func (s Redis) Copy(bucket string) Bucket {
	return MakeBucket(bucket)
}

func (s Redis) Set(key interface{}, value interface{}) error {
	//logs.Info("Set", `table=`, s, `key=`, key, `value=`, value)
	if _, ok := value.([]byte); !ok {
		v := fmt.Sprint(value)
		if v == "" {
			//logs.Info("赋值为空")
			_, err := rdb.HDel(ctx, s.String(), fmt.Sprint(key)).Result()
			if err != nil {
				return err
			}
		} else {
			_, err := rdb.HSet(ctx, s.String(), fmt.Sprint(key), fmt.Sprint(value)).Result()
			if err != nil {
				return err
			}
		}
	} else {
		if len(value.([]byte)) == 0 {
			_, err := rdb.HDel(ctx, s.String(), fmt.Sprint(key)).Result()
			if err != nil {
				return err
			}
		} else {
			_, err := rdb.HSet(ctx, s.String(), fmt.Sprint(key), fmt.Sprint(value)).Result()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (s Redis) Empty() (bool, error) {
	var value bool
	v, err := rdb.HLen(ctx, s.String()).Result()
	if v == 0 {
		value = true
	} else {
		value = false
	}
	return value, err
	//panic("implement me")
	//return true,err
}

func (s Redis) Size() (int64, error) {
	//TODO implement me
	panic("implement me")
	//err := rdb.HGetAll(ctx, s.String()).()
}

func (s Redis) Delete() error {
	err := rdb.HDel(ctx, s.String()).Err()
	//_, err := rdb.HDel(ctx, s.String()).Result()
	//TODO implement me
	//panic("implement me")
	return err
}

func (s Redis) Buckets() ([][]byte, error) {
	//TODO implement me
	panic("implement me")
}

func (s Redis) GetString(kv ...interface{}) string {
	var key, value string
	for i := range kv {
		if i == 0 {
			key = fmt.Sprint(kv[0])
		} else {
			value = fmt.Sprint(kv[1])
		}
	}
	//b := tx.Bucket([]byte(s))      // 判断要创建的mytable是否存在
	//if b == nil {
	//	return nil
	//}
	//v, _ := rdb.HGet(ctx, s.String(), key).Result()

	//if v != "" {
	//	return v
	//} else {
	//	return value
	//}
	if v, _ := rdb.HGet(ctx, s.String(), fmt.Sprint(key)).Result(); v != "" {
		value = v
	}
	//logs.Info("GetString", `table=`, s, `key=`, key, `返回值=`, value)
	return value
}

func (s Redis) GetBytes(key string) []byte {
	//v, _ := rdb.HGet(ctx, s.String(), key).Bytes()
	var value []byte
	if v, _ := rdb.HGet(ctx, s.String(), key).Bytes(); v != nil {
		value = v
	}
	//logs.Info("GetBytes", `table=`, s, `key=`, key, `返回值=`, value)
	return value
	/*
		if v != "" {
			return []byte(v)
		} else {
			return []byte("")
		}
	*/
}

func (s Redis) GetInt(key interface{}, vs ...int) int {
	var value int
	if len(vs) != 0 {
		value = vs[0]
	}
	v, _ := rdb.HGet(ctx, s.String(), fmt.Sprint(key)).Int()
	//v, _ := queryData(s.String(), fmt.Sprint(key))
	//v := Int(string(b.Get([]byte(fmt.Sprint(key)))))
	if v != 0 {
		value = v
	}
	//logs.Info("GetInt", `table=`, s, `kv=`, key, `vs=`, vs, `v=`, v, `返回值=`, value)
	return value
	/*
		if v != 0 {
			val, err := strconv.Atoi(string(v))
			logs.Info("GetInt", `val=`, val)
			if err != nil {
				return value
			} else {
				return val
			}
		} else {
			return value
		}
	*/
}

func (s Redis) GetBool(key interface{}, vs ...bool) bool {
	var value bool
	if len(vs) != 0 {
		value = vs[0]
	}
	v, _ := rdb.HGet(ctx, s.String(), fmt.Sprint(key)).Bool()
	if v == true {
		value = true
	} else if v == false {
		value = false
	}
	//logs.Info("GetBool", `table=`, s.String(), `key=`, key, `vs=`, vs, `返回值=`, value)
	return value
}

func (s Redis) Foreach(f func(k, v []byte) error) {
	//logs.Info("查询数据库表", s.String(), "的所有数据")
	if s == "*" {
		rows, err := rdb.Keys(ctx, "*").Result()
		//logs.Info("查询数据库表", rows, "的所有数据")
		//查询数据库表 [wxmp sillyGirl otto jk_hd qq frps] 的所有数据
		if err != nil {
			panic(err)
		}
		var result = make([]KeyValueMap, 0)
		for _, v := range rows {
			result = append(result, KeyValueMap{v, "0"})
			//fmt.Printf("key:%v value:%v\n", k, v)
		}
		//logs.Info("查询数据库某表的所有数据", result)
		//查询数据库某表的所有数据 [{wxmp 0} {sillyGirl 0} {otto 0} {jk_hd 0} {qq 0} {frps 0}]
		//kvm, _ := queryDatas(s.String())
		for _, kv := range result {
			f([]byte(kv.key), []byte(kv.value))
		}
	} else {
		rows, err := rdb.HGetAll(ctx, s.String()).Result()
		//logs.Info("查询数据库表", rows, "的所有数据")
		//查询数据库表 map[access_token:871622164 auto_friend:false #设置是否自动同意好友请求,似乎没用，不用在意。 default_bot:97694797 maste
		//rs:97694797&84896150 notifier:97694797&84896150 onGroups:935895893 onself:true #设置是否对监听自身消息 tempMessageGroupCode:935895893] 的所有数据
		if err != nil {
			panic(err)
		}
		// 强制排序
		keys := []string{}
		// 得到各个key
		for key1 := range rows {
			keys = append(keys, key1)
		}
		// 给key排序，按照字符串排序
		sort.Sort(sort.StringSlice(keys))

		var result = make([]KeyValueMap, 0)
		for _, key := range keys {
			result = append(result, KeyValueMap{key, rows[key]})
			//fmt.Printf("key = %v,value = %v\n", key, rows[key])
		}
		//排序结束
		/*
			for k, v := range rows {
				result = append(result, KeyValueMap{k, v})
				//fmt.Printf("key:%v value:%v\n", k, v)
			}*/
		//logs.Info("查询数据库某表的所有数据", result, err)
		//查询数据库某表的所有数据 [{tempMessageGroupCode 935895893} {onGroups 935895893} {auto_friend false #设置是否自动同意好友请求,似乎没
		//用，不用在意。} {onself true #设置是否对监听自身消息} {access_token 871622164} {notifier 97694797&84896150} {default_bot 97694797} {masters 97694797&84896150}]
		//kvm, _ := queryDatas(s.String())
		for _, kv := range result {
			f([]byte(kv.key), []byte(kv.value))
		}
	}

}

/*
//将结构体更新或存储到持久化表中
func (s3 Redis) Create(i interface{}) error {
	//logs.Error("进入数据库create函数")
	s := reflect.ValueOf(i).Elem()
	id := s.FieldByName("ID")
	sequence := s.FieldByName("Sequence")

	//如果表不存在，就创建
	b, _ := checkTable(s3.String())
	if !b { //id为int型
		err := createTable(s3.String())
		if err != nil {
			return err
		}
	}

	//如果id为int类型
	if _, ok := id.Interface().(int); ok {
		key := id.Int()
		sq, err := s3.NextSequence()
		if err != nil {
			return err
		}
		if key == 0 {
			key = int64(sq)
			id.SetInt(key)
		}
		if sequence != reflect.ValueOf(nil) {
			sequence.SetInt(int64(sq))
		}
		buf, err := json.Marshal(i)
		if err != nil {
			return err
		}
		return s3.Set(fmt.Sprintf("%d", key), string(buf))
	} else { //id为string类型
		key := id.String()
		sq, err := s3.NextSequence()
		//logs.Error(sq, err)
		if err != nil {
			return err
		}
		if key == "" {
			key = fmt.Sprint(sq)
			id.SetString(key)
		}
		if sequence != reflect.ValueOf(nil) {
			sequence.SetInt(int64(sq))
		}
		buf, err := json.Marshal(i)
		if err != nil {
			return err
		}
		return s3.Set(key, string(buf))
	}
}
*/

/*
//获取数据表中第一个元素并解析到i
func (s3 Redis) First(i interface{}) error {

	s := reflect.ValueOf(i).Elem()
	id := s.FieldByName("ID")
	if v, ok := id.Interface().(int); ok {
		if bl, _ := checkTable(s3.String()); bl {
			err := errors.New("bucket not find")
			return err
		}
		data, err := queryData(s3.String(), fmt.Sprintf("%d", v))
		if err != nil {
			return err
		}
		if len(data) == 0 {
			err := errors.New("record not find")
			return err
		}
		return json.Unmarshal([]byte(data), i)

	} else {
		if v, ok := id.Interface().(string); !ok {
			err := errors.New("bucket not find")
			return err
		} else {
			data, err := queryData(s3.String(), v)
			if err != nil {
				return err
			}
			if len(data) == 0 {
				err := errors.New("record not find")
				return err
			}
			return json.Unmarshal([]byte(data), i)
		}

	}
}
*/
