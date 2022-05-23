package tihall

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/c4pt0r/log"
)

var (
	ErrNameExists = fmt.Errorf("name exists")
)

var (
	db       *sql.DB
	updateQ  chan string
	hallName string
)

const (
	MAX_BATCH_SIZE     = 100
	MAX_IDLE_TIME      = 5 * time.Second
	INACTIVE_THRESHOLD = 2 * MAX_IDLE_TIME
)

func Init(dsn string, hallName string) error {
	if db != nil {
		db.Close()
		return nil
	}
	var err error
	db, err = sql.Open("mysql", dsn)
	if err != nil {
		return err
	}

	stmt := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS hall_%s (
			name VARCHAR(255) NOT NULL,
			content TEXT NOT NULL,
			last_alive DATETIME NOT NULL,
			PRIMARY KEY (name),
			KEY(last_alive)
		)
	`, hallName)
	_, err = db.Exec(stmt)
	if err != nil {
		return err
	}

	updateQ = make(chan string, MAX_BATCH_SIZE)
	go heartbeatWorker()
	return nil
}

func GC() {
	for {
		time.Sleep(MAX_IDLE_TIME)
		stmt := fmt.Sprintf(`
			DELETE FROM hall_%s WHERE last_alive < NOW() - INTERVAL ? SECOND
		`, hallName)
		_, err := db.Exec(stmt, INACTIVE_THRESHOLD.Seconds())
		if err != nil {
			fmt.Println("gc failed:", err)
		}
	}
}

func update(jobs []string) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	stmt := fmt.Sprintf(`
		UPDATE hall_%s SET last_alive = NOW() WHERE name = ?
	`, hallName)
	for _, job := range jobs {
		_, err := tx.Exec(stmt, job)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func heartbeatWorker() {
	log.I("heartbeat worker started")
	var jobs []string
	for job := range updateQ {
		jobs = append(jobs, job)
		if len(jobs) >= MAX_BATCH_SIZE {
			err := update(jobs)
			if err != nil {
				fmt.Println("update failed:", err)
			}
			jobs = nil
		}
	}
}

func Register(name string, content string) error {
	if IsAlive(name) {
		return ErrNameExists
	}
	log.I("registering:", name)

	stmt := fmt.Sprintf(`
		INSERT INTO hall_%s (name, content, last_alive) VALUES (?, ?, NOW())
	`, hallName)

	_, err := db.Exec(stmt, name, content)

	go func() {
		for {
			updateQ <- name
			time.Sleep(MAX_IDLE_TIME)
		}
	}()
	return err
}

func Unregister(name string) error {
	if !IsAlive(name) {
		return nil
	}
	stmt := fmt.Sprintf(`
		DELETE FROM hall_%s WHERE name = ?
	`, hallName)
	_, err := db.Exec(stmt, name)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	return nil
}

func ListAll() ([]string, error) {
	stmt := fmt.Sprintf(`
		SELECT name FROM hall_%s
	`, hallName)
	rows, err := db.Query(stmt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		err := rows.Scan(&name)
		if err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, nil
}

func IsAlive(name string) bool {
	stmt := fmt.Sprintf(`
		SELECT last_alive FROM hall_%s WHERE name = ? AND last_alive > NOW() - INTERVAL ? SECOND
	`, hallName)
	rows, err := db.Query(stmt, name, INACTIVE_THRESHOLD.Seconds())
	if err != nil {
		return false
	}
	defer rows.Close()
	return rows.Next()
}
