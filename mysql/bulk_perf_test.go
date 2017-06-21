package main

import (
	"database/sql"
	"fmt"
	"strconv"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var db *sql.DB

func init() {

	var err error

	db, err = sql.Open("mysql", "root:passwd@tcp(127.0.0.1:3306)/test?charset=utf8")
	checkErr(err)

	db.SetMaxOpenConns(2000)
	db.SetMaxIdleConns(1000)
	db.SetConnMaxLifetime(time.Minute * 60)
	err = db.Ping()
	checkErr(err)

	createTable()
}

// 使用For循环执行
func forInsert() {
	start := time.Now()

	stmt, err := db.Prepare(`INSERT user (user_name,user_age,user_sex) values (?,?,?)`)
	checkErr(err)
	for i := 0; i < 10000; i++ {
		name := "tony" + strconv.Itoa(i)
		_, err := stmt.Exec(name, i, 1)
		checkErr(err)
	}

	delta := time.Now().Sub(start).String()
	fmt.Println("For Insert Total Time: ", delta)
}

// 在一个事物内循环执行
func withTxInsert() {
	start := time.Now()

	tx, err := db.Begin()
	checkErr(err)

	stmt, err := tx.Prepare(`INSERT user (user_name,user_age,user_sex) values (?,?,?)`)
	checkErr(err)

	for i := 0; i < 10000; i++ {
		name := "tony" + strconv.Itoa(i)
		_, err := stmt.Exec(name, i, 1)
		checkErr(err)
	}

	if err != nil {
		err := tx.Rollback()
		checkErr(err)
		return
	}

	err = tx.Commit()
	checkErr(err)

	delta := time.Now().Sub(start).String()
	fmt.Println("Bulk With Transaction Insert Total Time: ", delta)
}

// 构造一条Insert语句批量提交
func bulkoneInsert() {
	start := time.Now()

	sql := "INSERT INTO `user` (`user_name`,`user_age`,`user_sex`) VALUES "
	for i := 0; i < 10000; i++ {
		name := "tony" + strconv.Itoa(i)
		if i < 9999 {
			sql += fmt.Sprintf("('%s','%d','%d'),", name, i, 1)
		} else {
			sql += fmt.Sprintf("('%s','%d','%d');", name, i, 1)
		}
	}

	// fmt.Println(sql)
	tx, err := db.Begin()
	checkErr(err)
	_, err = tx.Exec(sql)
	if err == nil {
		tx.Commit()
	} else {
		fmt.Println(err)
		tx.Rollback()
	}

	delta := time.Now().Sub(start).String()
	fmt.Println("Bulk One Insert Total Time: ", delta)
}

// 循环更新
// UPDATE table SET column1=?,column2=? WHERE column=?
func withTxUpdate() {
	start := time.Now()

	tx, err := db.Begin()
	checkErr(err)

	stmt, err := tx.Prepare("UPDATE `user` SET `user_name`=? WHERE `user_id`=?;")
	checkErr(err)

	for i := 0; i < 10000; i++ {
		name := "forupdate" + strconv.Itoa(i)
		_, err := stmt.Exec(name, i+1)
		checkErr(err)
	}

	if err != nil {
		err := tx.Rollback()
		checkErr(err)
		return
	}

	err = tx.Commit()
	checkErr(err)

	delta := time.Now().Sub(start).String()
	fmt.Println("Bulk With Transaction Update Total Time: ", delta)
}

// 标准Update语句更新
// UPDATE categories
//     SET dingdan = CASE id
//         WHEN 1 THEN 3
//         WHEN 2 THEN 4
//         WHEN 3 THEN 5
//     END,
//     title = CASE id
//         WHEN 1 THEN 'New Title 1'
//         WHEN 2 THEN 'New Title 2'
//         WHEN 3 THEN 'New Title 3'
//     END
// WHERE id IN (1,2,3)
func bulkStandardUpdate() {
	start := time.Now()

	core := ""
	where := ""
	for i := 0; i < 10000; i++ {
		name := "standardupdate" + strconv.Itoa(i)
		core += fmt.Sprintf("WHEN '%d' THEN '%s' ", i+1, name)
		if i == 0 {
			where += fmt.Sprintf("'%d'", i+1)
		} else {
			where += fmt.Sprintf(",'%d'", i+1)
		}
	}

	sql := fmt.Sprintf("UPDATE `user` SET `user_name`= CASE `user_id` %s END WHERE `user_id` IN (%s)", core, where)

	tx, err := db.Begin()
	checkErr(err)
	_, err = tx.Exec(sql)
	if err == nil {
		tx.Commit()
	} else {
		fmt.Println(err)
		tx.Rollback()
	}

	delta := time.Now().Sub(start).String()
	fmt.Println("Bulk Standard Update Total Time: ", delta)
}

// insert into语句更新
// INSERT INTO test_tbl (id,dr) VALUES (1,'2'),(2,'3'),...(x,'y') ON DUPLICATE KEY UPDATE dr=values(dr);
func bulkInsertIntoUpdate() {
	start := time.Now()

	core := ""
	for i := 0; i < 10000; i++ {
		name := "insertintoupdate" + strconv.Itoa(i)
		if i == 0 {
			core += fmt.Sprintf("('%d', '%s')", i+1, name)
		} else {
			core += fmt.Sprintf(",('%d', '%s')", i+1, name)
		}
	}

	sql := fmt.Sprintf("INSERT INTO `user` (`user_id`, `user_name`) VALUES %s ON DUPLICATE KEY UPDATE `user_name`=values(`user_name`);", core)

	tx, err := db.Begin()
	checkErr(err)
	_, err = tx.Exec(sql)
	if err == nil {
		tx.Commit()
	} else {
		fmt.Println(err)
		tx.Rollback()
	}

	delta := time.Now().Sub(start).String()
	fmt.Println("Bulk Insert Into Update Total Time: ", delta)
}

// replace inot语句更新
// REPLACE INTO test_tbl (id,dr) VALUES (1,'2'),(2,'3'),...(x,'y');
func bulkReplaceIntoUpdate() {
	start := time.Now()

	core := ""
	for i := 0; i < 10000; i++ {
		name := "replaceintoupdate" + strconv.Itoa(i)
		if i == 0 {
			core += fmt.Sprintf("('%d', '%s')", i+1, name)
		} else {
			core += fmt.Sprintf(",('%d', '%s')", i+1, name)
		}
	}

	sql := fmt.Sprintf("REPLACE INTO `user` (`user_id`, `user_name`) VALUES %s;", core)

	tx, err := db.Begin()
	checkErr(err)
	_, err = tx.Exec(sql)
	if err == nil {
		tx.Commit()
	} else {
		fmt.Println(err)
		tx.Rollback()
	}

	delta := time.Now().Sub(start).String()
	fmt.Println("Bulk Replace Into Update Total Time: ", delta)
}

func createTable() {
	db, err := sql.Open("mysql", "root:passwd@tcp(127.0.0.1:3306)/test?charset=utf8")
	checkErr(err)
	table := `CREATE TABLE IF NOT EXISTS test.user (
 user_id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '用户编号',
 user_name VARCHAR(45) NOT NULL COMMENT '用户名称',
 user_age TINYINT(3) UNSIGNED NOT NULL DEFAULT 0 COMMENT '用户年龄',
 user_sex TINYINT(3) UNSIGNED NOT NULL DEFAULT 0 COMMENT '用户性别',
 PRIMARY KEY (user_id))
 ENGINE = InnoDB
 AUTO_INCREMENT = 1
 DEFAULT CHARACTER SET = utf8
 COLLATE = utf8_general_ci
 COMMENT = '用户表'`
	if _, err := db.Exec(table); err != nil {
		checkErr(err)
	}
}

func checkErr(err error) {
	if err != nil {
		fmt.Println(err)
		panic(err)
	}
}

func main() {
	withTxUpdate()
	bulkStandardUpdate()
	bulkInsertIntoUpdate()
	bulkReplaceIntoUpdate()
}
