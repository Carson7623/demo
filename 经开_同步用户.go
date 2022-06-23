package main

import (
	"bytes"
	"context"
	"crypto/aes"
	cip "crypto/cipher"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/bitly/go-simplejson"
	_ "github.com/go-sql-driver/mysql"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"golang.org/x/crypto/bcrypt"
)

const (
	userName = "k12_test_new"
	password = "d6rf6%jpd12$5s"
	port     = "3308"
	ip       = "127.0.0.1"
	dbName   = "oouc_jingkai"
	RegionID = 3515 //经开区region_id

	Iv  = "0000000000000000" //AES加密偏移量
	Key = "zyyj85169336zyyj" //AES加密秘钥

)

var DB *sql.DB

func mains() {

	path := strings.Join([]string{userName, ":", password, "@tcp(", ip, ":", port, ")/", dbName, "?charset=utf8"}, "")
	DB, _ = sql.Open("mysql", path)
	DB.SetConnMaxLifetime(100)
	DB.SetMaxIdleConns(10)
	if err := DB.Ping(); err != nil {
		fmt.Println("opon database fail")
		return
	}

Begin:
	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
	}
	dialer.SASLMechanism = plain.Mechanism{
		Username: "adminplain",   // 用户名
		Password: "admin-secret", // 密码
	}
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"121.36.211.137:9092"}, // domain 是kafka地址  "kafka.zyyj.com.cn:9092"
		GroupID:  "oouc_test_24",                  //ID,需唯一
		Topic:    "user",                          //topic 是需要读取的 topic .
		MinBytes: 10e3,                            // 10KB
		MaxBytes: 10e6,                            // 10MB
		Dialer:   dialer,
	})

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			fmt.Println("读取数据失败")
			r.Close()
			goto Begin
		}
		topic := string(m.Key)
		value := string(m.Value)
		jsondata, err := simplejson.NewJson([]byte(m.Value))
		if err != nil {
			continue
		}
		user_type, _ := jsondata.Get("user_type").Int()
		user_types := jsondata.Get("user_types").MustArray()
		is_user_status := 0
		if len(user_types) > 0 {
			for _, element := range user_types {
				t, _ := element.(json.Number)
				v, _ := t.Int64()
				if Int642str(v) == "2" {
					is_user_status = 1
					break
				}
			}
		}
		if (topic == "add" && user_type != 2) || (topic == "edit" && is_user_status == 0) {
			Add_Log(r.Config().GroupID, "user", topic, value, "", 3)
			continue
		}

		uuid, _ := jsondata.Get("uuid").String()
		name, _ := jsondata.Get("name").String()
		mobile, _ := jsondata.Get("mobile").String()
		sex, _ := jsondata.Get("sex").Int()
		if sex == 2 {
			sex = 0
		}

		id_card_no, _ := jsondata.Get("id_card").String()
		id_card, err := AesEncrypt(id_card_no, Iv, []byte(Key))
		if err != nil {
			fmt.Println(err)
			continue
		}

		if topic == "add" {
			var user_id int
			sql := `SELECT user_id FROM users_info WHERE uuid = ?`
			row, err := DoQuery(DB, sql, uuid)
			if err != nil {
				fmt.Println(err)
				continue
			}

			t, _ := DB.Begin()
			if len(row) > 0 { //用户已存在，当成编辑处理;
				user_id = Str2int(GetInterfaceToString(row[0]["user_id"]))
				hash_psd := ""
				if mobile != "" {
					hash_psd = string(HashPassword(mobile[5:]))
				} else {
					hash_psd = string(HashPassword("Aa123456"))
				}

				sql = `UPDATE users SET username = ?, password = ? WHERE id = ?`
				_, err := t.Exec(sql, uuid, hash_psd, user_id)
				if err != nil {
					t.Rollback()
					Add_Log(r.Config().GroupID, "user", topic, value, "添加-用户失败", 2)
					continue
				}

				sql = `UPDATE users_info SET user_id = ?, name = ?, id_card =?, sex = ?, phone = ?, card_type = ? WHERE uuid = ?`
				_, err = t.Exec(sql, user_id, name, id_card, sex, mobile, 0, uuid)
				if err != nil {
					t.Rollback()
					Add_Log(r.Config().GroupID, "user", topic, value, "添加用户基本信息失败", 2)
					return
				}
			} else {
				hash_psd := ""
				if mobile != "" {
					hash_psd = string(HashPassword(mobile[5:]))
				} else {
					hash_psd = string(HashPassword("Aa123456"))
				}
				sql = `INSERT INTO users(username,password) VALUES (?,?)`
				res_user, err := t.Exec(sql, uuid, hash_psd)
				if err != nil {
					t.Rollback()
					Add_Log(r.Config().GroupID, "user", topic, value, "添加-插入用户名失败", 2)
					continue
				}
				user, _ := res_user.LastInsertId()
				user_id = int(user)

				sql = `INSERT INTO users_info(user_id,name,id_card,sex,phone,card_type,uuid) VALUES (?,?,?,?,?,?,?)`
				_, err = t.Exec(sql, user_id, name, id_card, sex, mobile, 0, uuid)
				if err != nil {
					t.Rollback()
					Add_Log(r.Config().GroupID, "user", topic, value, "添加用户基本信息失败", 2)
					return
				}
			}

			sql = `SELECT id FROM user_certificate WHERE user_id = ?`
			row, err = DoQuery(DB, sql, user_id)
			if err != nil {
				t.Rollback()
				return
			}
			if len(row) == 0 {
				sql = `INSERT INTO user_certificate (user_id) VALUES (?)`
				_, err = t.Exec(sql, user_id)
				if err != nil {
					t.Rollback()
					Add_Log(r.Config().GroupID, "user", topic, value, "插入机构管理员数据错误", 2)
					return
				}
			}
			t.Commit()
			Add_Log(r.Config().GroupID, "user", topic, value, "", 1)

		} else if topic == "edit" {
			columns, _ := jsondata.Get("columns").StringArray()
			if len(columns) == 0 {
				continue
			}
			var (
				nameUp   string
				sexUp    string
				mobileUp string
			)
			for _, v := range columns {
				switch v {
				case "name":
					nameUp = name
				case "sex":
					sexUp = Int2str(sex)
				case "mobile":
					mobileUp = mobile
				}
			}
			if uuid == "" && nameUp == "" && mobileUp == "" && sexUp == "" {
				Add_Log(r.Config().GroupID, "user", topic, value, "参数错误", 2)
				continue
			}

			sql := `SELECT user_id FROM users_info WHERE uuid = ?`
			row, err := DoQuery(DB, sql, uuid)
			if err != nil {
				Add_Log(r.Config().GroupID, "user", topic, value, "查询用户信息失败", 2)
				continue
			}

			t, _ := DB.Begin()
			var user_id int
			if len(row) == 0 { //没有数据，当新加处理；
				hash_psd := ""
				if mobile != "" {
					hash_psd = string(HashPassword(mobile[5:]))
				} else {
					hash_psd = string(HashPassword("Aa123456"))
				}

				sql = `INSERT INTO users(username,password) VALUES (?,?)`
				res_user, err := t.Exec(sql, uuid, hash_psd)
				if err != nil {
					t.Rollback()
					Add_Log(r.Config().GroupID, "user", topic, value, "编辑-添加用户失败", 2)
					continue
				}
				user, _ := res_user.LastInsertId()
				user_id = int(user)

				sql = `INSERT INTO users_info(user_id,name,id_card,sex,phone,card_type,uuid) VALUES (?,?,?,?,?,?,?)`
				_, err = t.Exec(sql, user_id, name, id_card, sex, mobile, 0, uuid)
				if err != nil {
					t.Rollback()
					Add_Log(r.Config().GroupID, "user", topic, value, "添加用户基本信息失败", 2)
					return
				}
			} else {
				user_id = Str2int(GetInterfaceToString(row[0]["user_id"]))
				var (
					whereUp  strings.Builder
					whereUp2 strings.Builder
				)
				if name != "" {
					whereUp.WriteString(`name = "` + name + `",`)
					whereUp2.WriteString(`user_name = "` + name + `",`)
				}
				if Int2str(sex) != "" {
					whereUp.WriteString(`sex = ` + Int2str(sex) + `,`)
				}
				if mobile != "" {
					whereUp.WriteString(`phone = ` + mobile + `,`)
				}
				if ups := whereUp.String(); ups != "" {
					sql = `UPDATE users_info SET ` + strings.TrimSuffix(ups, ",") + ` WHERE user_id = ?`
					_, err = t.Exec(sql, user_id)
					if err != nil {
						t.Rollback()
						Add_Log(r.Config().GroupID, "user", topic, value, "更新机构人员信息失败"+err.Error(), 2)
						continue
					}
				}

				if ups := whereUp2.String(); ups != "" {
					sql = `UPDATE production_collection SET ` + strings.TrimSuffix(ups, ",") + ` WHERE user_id = ?`
					_, err = t.Exec(sql, user_id)
					if err != nil {
						t.Rollback()
						Add_Log(r.Config().GroupID, "user", topic, value, "更新机构人员作品信息失败", 2)
						continue
					}
				}
			}

			sql = `SELECT id FROM user_certificate WHERE user_id = ?`
			row, err = DoQuery(DB, sql, user_id)
			if err != nil {
				t.Rollback()
				return
			}
			if len(row) == 0 {
				sql = `INSERT INTO user_certificate (user_id) VALUES (?)`
				_, err = t.Exec(sql, user_id)
				if err != nil {
					t.Rollback()
					Add_Log(r.Config().GroupID, "user", topic, value, "插入机构管理员数据错误", 2)
					return
				}
			}

			t.Commit()
			Add_Log(r.Config().GroupID, "user", topic, value, "", 1)
		} else if topic == "del" {
			sql := `SELECT user_id FROM users_info WHERE uuid = ?`
			row, err := DoQuery(DB, sql, uuid)
			if err != nil {
				Add_Log(r.Config().GroupID, "user", topic, value, "查询用户信息失败", 2)
				continue
			}
			if len(row) == 0 {
				Add_Log(r.Config().GroupID, "user", topic, value, "用户不存在", 2)
				continue
			}
			user_id := Str2int(GetInterfaceToString(row[0]["user_id"]))

			t, _ := DB.Begin()
			sql = `DELETE FROM user_group_relation WHERE user_id = ? `
			_, err = t.Exec(sql, user_id)
			if err != nil {
				t.Rollback()
				Add_Log(r.Config().GroupID, "user", topic, value, "删除用户关系失败", 2)
				continue
			}

			sql = `DELETE FROM users WHERE id = ?`
			_, err = t.Exec(sql, user_id)
			if err != nil {
				t.Rollback()
				Add_Log(r.Config().GroupID, "user", topic, value, "删除用户失败", 2)
				continue
			}

			sql = `DELETE FROM users_info WHERE user_id = ?`
			_, err = t.Exec(sql, user_id)
			if err != nil {
				t.Rollback()
				Add_Log(r.Config().GroupID, "user", topic, value, "删除用户信息失败", 2)
				continue
			}

			sql = `SELECT id FROM production_collection WHERE user_id = ?`
			row, err = DoQuery(DB, sql, user_id)
			if err != nil {
				t.Rollback()
				Add_Log(r.Config().GroupID, "user", topic, value, "查询作品集失败", 2)
				continue
			}

			if len(row) > 0 {
				var del_collections []string
				for _, value := range row {
					del_collections = append(del_collections, string(GetInterfaceToString(value["id"])))
				}

				sql = `DELETE FROM production_resource WHERE collection_id IN (` + strings.Join(del_collections, ",") + `)`
				_, err = t.Exec(sql)
				if err != nil {
					t.Rollback()
					Add_Log(r.Config().GroupID, "user", topic, value, "删除相关作品资源失败", 2)
					continue
				}

				sql = `DELETE FROM production_score_log WHERE collection_id IN (` + strings.Join(del_collections, ",") + `)`
				_, err = t.Exec(sql)
				if err != nil {
					t.Rollback()
					Add_Log(r.Config().GroupID, "user", topic, value, "删除相关作品分数记录失败", 2)
					continue
				}

				sql = `DELETE FROM production_winner WHERE collection_id IN (` + strings.Join(del_collections, ",") + `)`
				_, err = t.Exec(sql)
				if err != nil {
					t.Rollback()
					Add_Log(r.Config().GroupID, "user", topic, value, "删除相关获奖记录失败", 2)
					continue
				}

				sql = `DELETE FROM production_make_score WHERE collection_id IN (` + strings.Join(del_collections, ",") + `)`
				_, err = t.Exec(sql)
				if err != nil {
					t.Rollback()
					Add_Log(r.Config().GroupID, "user", topic, value, "删除相关作品分数记录失败", 2)
					continue
				}

				sql = `DELETE FROM production_reject WHERE collection_id IN (` + strings.Join(del_collections, ",") + `)`
				_, err = t.Exec(sql)
				if err != nil {
					t.Rollback()
					Add_Log(r.Config().GroupID, "user", topic, value, "删除相关作品驳回情况失败", 2)
					continue
				}

				sql = `DELETE FROM production_collection WHERE user_id = ?`
				_, err = t.Exec(sql, user_id)
				if err != nil {
					t.Rollback()
					Add_Log(r.Config().GroupID, "user", topic, value, "删除相关作品集失败", 2)
					continue
				}
			}

			sql = `DELETE FROM user_certificate WHERE user_id = ?`
			_, err = t.Exec(sql, user_id)
			if err != nil {
				t.Rollback()
				Add_Log(r.Config().GroupID, "user", topic, value, "删除证书管理员失败", 2)
				continue
			}

			t.Commit()
			Add_Log(r.Config().GroupID, "user", topic, value, "", 1)
		}
	}
}

//AES-CBC加密
func AesEncrypt(encodeStr, iv string, key []byte) (string, error) {
	encodeBytes := []byte(encodeStr)
	//根据key 生成密文
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}

	blockSize := block.BlockSize()
	encodeBytes = PKCS5Padding(encodeBytes, blockSize)

	blockMode := cip.NewCBCEncrypter(block, []byte(iv))
	crypted := make([]byte, len(encodeBytes))
	blockMode.CryptBlocks(crypted, encodeBytes)

	return base64.StdEncoding.EncodeToString(crypted), nil
}

func PKCS5Padding(ciphertext []byte, blockSize int) []byte {
	padding := blockSize - len(ciphertext)%blockSize
	//填充
	padtext := bytes.Repeat([]byte{byte(padding)}, padding)

	return append(ciphertext, padtext...)
}

func Str2int(str string) int {
	t, err := strconv.Atoi(str)
	if err != nil {
		return 0
	}
	return t
}

func Int2str(t int) string {
	str := strconv.Itoa(t)
	return str
}

func DoQuery(db *sql.DB, sqlInfo string, args ...interface{}) ([]map[string]interface{}, error) {
	rows, err := db.Query(sqlInfo, args...)
	if err != nil {
		return nil, err
	}
	columns, _ := rows.Columns()
	columnLength := len(columns)
	cache := make([]interface{}, columnLength) //临时存储每行数据
	for index, _ := range cache {              //为每一列初始化一个指针
		var a interface{}
		cache[index] = &a
	}
	var list []map[string]interface{} //返回的切片
	for rows.Next() {
		_ = rows.Scan(cache...)

		item := make(map[string]interface{})
		for i, data := range cache {
			item[columns[i]] = *data.(*interface{}) //取实际类型
		}
		list = append(list, item)
	}
	_ = rows.Close()
	return list, nil
}

func HashPassword(password string) []byte {
	pass := []byte(password)
	hash, _ := bcrypt.GenerateFromPassword(pass, 8)
	if len(hash) == 60 {
		return hash
	}
	return []byte("*")
}

func GetInterfaceToString(value interface{}) string {
	var key string
	if value == nil {
		return key
	}
	switch value.(type) {
	case float64:
		ft := value.(float64)
		key = strconv.FormatFloat(ft, 'f', -1, 64)
	case float32:
		ft := value.(float32)
		key = strconv.FormatFloat(float64(ft), 'f', -1, 64)
	case int:
		it := value.(int)
		key = strconv.Itoa(it)
	case uint:
		it := value.(uint)
		key = strconv.Itoa(int(it))
	case int8:
		it := value.(int8)
		key = strconv.Itoa(int(it))
	case uint8:
		it := value.(uint8)
		key = strconv.Itoa(int(it))
	case int16:
		it := value.(int16)
		key = strconv.Itoa(int(it))
	case uint16:
		it := value.(uint16)
		key = strconv.Itoa(int(it))
	case int32:
		it := value.(int32)
		key = strconv.Itoa(int(it))
	case uint32:
		it := value.(uint32)
		key = strconv.Itoa(int(it))
	case int64:
		it := value.(int64)
		key = strconv.FormatInt(it, 10)
	case uint64:
		it := value.(uint64)
		key = strconv.FormatUint(it, 10)
	case string:
		key = value.(string)
	case []byte:
		key = string(value.([]byte))
	default:
		newValue, _ := json.Marshal(value)
		key = string(newValue)
	}

	return key
}

func Add_Log(group_id, topic, topickey, topicvalue, msg string, status int) string {
	sql := `INSERT INTO kafka_log(group_id,topic,topickey,topicvalue,msg,status)VALUES(?,?,?,?,?,?)`
	_, err := DB.Exec(sql, group_id, topic, topickey, topicvalue, msg, status)
	if err != nil {
		return "插入kafka_log失败"
	}
	return ""
}

func Int642str(t int64) string {
	str := strconv.FormatInt(int64(t), 10)
	return str
}
