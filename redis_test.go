package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"testing"
	"time"
)

var (
	r      = new(Redis)
	option = Option{
		Host:     "127.0.0.1:6379",
		Password: "redispw",
		Db:       0,
	}
)

func TestRedis_Set(t *testing.T) {

	r = Init(context.Background(), &option)
	members, err := r.SMember("key_list")
	if err != nil {
		panic(err)
	}
	fmt.Println(members)
}

func TestRedis_Scan(t *testing.T) {

	r = Init(context.Background(), &option)
	members, err := r.KeyList("metric_*", 100)
	if err != nil {
		panic(err)
	}
	fmt.Println(len(members))
	fmt.Println(members)
}

func TestRedis_Close(t *testing.T) {

	r = Init(context.Background(), &option)
	err := r.Pool.Close()
	if err != nil {
		fmt.Println(err)
	}
}

func TestRedis_Incr(t *testing.T) {

	r = Init(context.Background(), &option)
	v, err := r.Incr("incr_test")
	if err != nil {
		panic(err)
	}
	fmt.Println(v)
	err = r.Pool.Close()

}

func TestRedis_TTL(t *testing.T) {

	r = Init(context.Background(), &option)
	v, err := r.TTL("incr_test")
	if err != nil {
		panic(err)
	}

	fmt.Println(v.Seconds())
	err = r.Pool.Close()

}

func TestRedis_HSETAndHGet(t *testing.T) {
	key := "6a052cd2-2b25-44ea-b167-ce91e9cfa388"
	value := `{
		"appId": "6a052cd2-2b25-44ea-b167-ce91e9cfa388",
		"projectID": "30f39a0e-c9b4-42f5-9a4d-411736e1229a",
		"accountID": "1229276561698701317",
		"desc": "看问题的应用",
		"name": "看问题的应用",
		"appKeyStatus": "ENABLED",
		"appKey": "rqkmetjlujnjpaiagfuhtxkr",
		"createdAt": "2021-04-15T15:09:08+08:00",
		"updatedAt": "2021-04-15T15:09:08+08:00"
	  }`

	hashValue := make(map[string]interface{})

	err := json.Unmarshal([]byte(value), &hashValue)
	if err != nil {
		t.Fatal(err)
	}
	r = Init(context.Background(), &option)

	err = r.HSet(key, hashValue)
	if err != nil {
		t.Fatal(err)
	}

	values, err := r.HGetAll(key)
	if err != nil {
		t.Fatal(err)
	}

	for k, v := range values {
		fmt.Println(k, v)
	}

	id, ok := values["appId"]
	if !ok {
		t.Fatal("id field not found")
	}

	if id != key {
		t.Fatal("key not match")
	}

}

func TestRedis_HGet(t *testing.T) {
	key := "test_hget_key"
	field := "test_hget_key_field"
	value := map[string]interface{}{field: "test_hget_key_value"}

	r = Init(context.Background(), &option)
	err := r.HSet(key, value)
	if err != nil {
		t.Fatal(err)
	}

	v, err := r.HGet(key, field)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(string(v))

}

func TestRedis_MqPublish(t *testing.T) {
	err := r.Pool.Ping(r.ctx).Err()
	if err != nil {
		t.Fatal(err)
	}

	var count = 1

	stream := "test-stream"
	for {
		time.Sleep(50 * time.Millisecond)

		msg := fmt.Sprintf("this is a test message: %d", count)

		err = r.AddMessage(stream, []byte(msg))

		if err != nil {
			t.Fatal(err)
		}

		count++
	}
}

func TestRedis_MsgSubscribe(t *testing.T) {
	err := r.Pool.Ping(r.ctx).Err()
	if err != nil {
		t.Fatal(err)
	}

	stream := "test-stream"
	group := "test-stream-group-1"
	_ = r.CreateGroup(stream, group)

	for {
		msgs, err := r.ReadGroupBlock(stream, group, "consumer_id_test", 10, 50)
		if err != nil {
			t.Fatal(err)
		}

		msgIDs := make([]string, 0)

		for _, msg := range msgs {
			log.Println(msg.ID, msg.Values)
			msgIDs = append(msgIDs, msg.ID)
		}

		err = r.AckMessage(stream, group, msgIDs...)
		if err != nil {
			t.Fatal(err)
		}

		err = r.DeleteMessage(stream, msgIDs...)
		if err != nil {
			t.Fatal(err)
		}
	}
}
