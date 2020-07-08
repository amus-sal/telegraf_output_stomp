package main

import (
	"crypto/tls"
	"flag"
	"net"

	"github.com/go-stomp/stomp"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/outputs"
	"github.com/influxdata/telegraf/plugins/serializers"
)


//STOMP ...
type STOMP struct {
	Host      string `toml:"host"`
	Username  string
	Password  string
	QueueName string `toml:"queueName"`
	SSL       bool   `toml:"ssl"`
	Conn      *tls.Conn
	Stomp     *stomp.Conn
	serialize serializers.Serializer
}

//Connect ...
func (q *STOMP) Connect() error {
	var err error
	if q.SSL == true {
		q.Conn, err = tls.Dial("tcp", q.Host, &tls.Config{})
	} else {
		q.Conn, err = net.Dial("tcp", q.Host)
	}
	if err != nil {
		println("cannot connect to server", err.Error())
		return err
	}

	ConnOtp := q.buildOtp(stomp.ConnOpt)
	q.Stomp, err = stomp.Connect(q.Conn, ConnOtp)
	if err != nil {
		println(err.Error())
		return err
	}
	println("STOMP Connected...")
	return nil
}

func (q *STOMP) buildOtp(ConnOpt *stomp.ConnOpt) *stomp.ConnOpt {
	ConnOpt.Login(q.Username, q.Password)
	ConnOpt.HeartBeat(0, 0))
	return ConnOpt
}

//SetSerializer ...
func (q *STOMP) SetSerializer(serializer serializers.Serializer) {
	q.serialize = serializer
	println("start serialize")
}

//Write ...
func (q *STOMP) Write(metrics []telegraf.Metric) error {
	println("Start Writing...")
	for _, metric := range metrics {
		println("Start looping for metrics before serializer...")
		values, err := q.serialize.Serialize(metric)
		println("After serializer...", err)
		if err != nil {
			return err
		}
		println("Field values are: ", values)
		err = q.Stomp.Send("kannel_log_test", "text/plain",
			[]byte(values), nil)
		if err != nil {
			println("failed to send to server", err)
			return err
		}
	}
	println("sender finished")
	return nil
}

//Close ...
func (q *STOMP) Close() error {
	println("Closiong is starting .....")
	q.Stomp.Disconnect()
	q.Conn.Close()
	return nil
}

//SampleConfig ...
func (q *STOMP) SampleConfig() string {
	return `ok = true`
}

//Description ...
func (q *STOMP) Description() string {
	return "Hello From Stomp"
}
func init() {
	outputs.Add("stomp", func() telegraf.Output { return &STOMP{} })
}
