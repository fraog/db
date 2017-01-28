// Copyright Sinking Ship Games
// All Rights Reserved

//Package db contains some simple structures and function for use with a 2-D game.
package db

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

type TableManager struct {
	database *sql.DB
	table    string
	//valueMap map[rune][2]string
	valueMap map[rune][2]interface{}

	//memoization
	insertReq string
	deleteReq string
	readReqs  map[string]string
	writeReqs map[string]string
}

func NewTableManager(db *sql.DB, table string) *TableManager {
	return &TableManager{db, table, make(map[rune][2]interface{}), "", "", make(map[string]string), make(map[string]string)}
}

func NewSQLiteTableManager(filename string, table string) (*TableManager, error) {
	db, e := sql.Open("sqlite3", filename)
	if e != nil {
		return nil, e
	}
	return NewTableManager(db, table), nil
}

func (m *TableManager) AddField(k rune, label string, defVal interface{}) {
	m.valueMap[k] = [2]interface{}{label, defVal}
	m.insertReq = ""
	//TODO: clear readReqs and writeReqs?
}

func (m *TableManager) RemoveField(k rune) {
	delete(m.valueMap, k)
}

func (m *TableManager) GetInsertRequest() string {

	vstart := len(m.valueMap) * 2
	strVals := make([]interface{}, len(m.valueMap)*4+3)

	strVals[0] = "INSERT INTO "
	strVals[1] = m.table
	strVals[2] = "("
	i := 3
	for _, v := range m.valueMap {
		strVals[i] = v[0]
		strVals[vstart+i] = v[1]
		if i < vstart {
			strVals[i+1] = ", "
			strVals[vstart+i+1] = ", "
		}
		i += 2
	}
	strVals[vstart+2] = ") VALUES ("
	strVals[len(strVals)-1] = ")"

	return fmt.Sprint(strVals...)
}

func (m *TableManager) GetDeleteRequest() string {
	return fmt.Sprintf("DELETE FROM %s WHERE ID = %s", m.table, "%v")
}

func (m *TableManager) GetReadRequest(keys string) string {

	if _, ok := m.readReqs[keys]; !ok {
		idents := make([]string, len(keys))
		for i, v := range keys {
			if v == 'i' {
				idents[i] = "id"
				continue
			}
			if _, ok := m.valueMap[v]; ok {
				idents[i] = m.valueMap[v][0].(string)
			} else {
				fmt.Println("VALUE NOT IN VALUEMAP:", v)
				fmt.Println(v)
			}
		}
		m.readReqs[keys] = fmt.Sprintf("SELECT %s FROM %s WHERE %s", strings.Join(idents, ","), m.table, "%s")
	}
	return m.readReqs[keys]
}

func (m *TableManager) GetWriteRequest(keys string) string {
	if _, ok := m.writeReqs[keys]; !ok {
		//update table set label = value, label1 = value1 where id = id
		strVals := make([]interface{}, len(keys)+2)
		strVals[0] = fmt.Sprintf("UPDATE %s SET ", m.table)
		i := 1
		for _, v := range keys {
			if i < len(keys) {
				/*
					switch m.valueMap[v][1].(type) {
					default:
						strVals[i] = fmt.Sprint(m.valueMap[v][0], " = %v, ")
					case string:
						strVals[i] = fmt.Sprint(m.valueMap[v][0], " = '%s', ")
					}
				*/
				strVals[i] = fmt.Sprint(m.valueMap[v][0], " = ?, ")
			} else {
				/*
					switch m.valueMap[v][1].(type) {
					default:
						strVals[i] = fmt.Sprint(m.valueMap[v][0], " = %v")
					case string:
						strVals[i] = fmt.Sprint(m.valueMap[v][0], " = '%s'")
					}
				*/
				strVals[i] = fmt.Sprint(m.valueMap[v][0], " = ?")
			}
			i++
		}
		//strVals[len(strVals)-1] = " WHERE %%s"
		strVals[len(strVals)-1] = " WHERE %s"
		m.writeReqs[keys] = fmt.Sprint(strVals...)
		fmt.Printf("WRITEINTO:%s\n", m.writeReqs[keys])

	}
	return m.writeReqs[keys]
}

/*
//TODO: do all the string parsing?
func (m *TableManager) Write(B []byte) (int, error) {
	return 0, nil
}
*/

func (m *TableManager) WriteInto(sel string, keys string, values ...interface{}) error {
	req := m.GetWriteRequest(keys) //fmt.Sprintf(m.GetWriteRequest(keys), values...)

	//fmt.Printf("REQ:%s\n", req)
	//fmt.Printf("SELECTOR:%s\n", sel)
	//fmt.Printf("WRITEINTO:%s\n", fmt.Sprintf(req, sel))
	//fmt.Printf("EXEC:\n\n")
	//_, e := m.database.Exec(fmt.Sprintf(req, sel)) //,id)
	_, e := m.database.Exec(fmt.Sprintf(req, sel), values...)
	if e != nil {
		fmt.Println("ERROR EXECUTING MYSQL")
		return e
	}
	return nil
}

func (m *TableManager) ReadInto(sel string, keys string, objects ...interface{}) error {
	req := fmt.Sprintf(m.GetReadRequest(keys), sel)
	//fmt.Println(req)
	e := m.database.QueryRow(req).Scan(objects...)
	if e != nil {
		return e
	}
	return nil
}

func (m *TableManager) Read(sel string, keys string) ([]interface{}, error) {

	//Create storage for result
	result := make([]interface{}, len(keys))
	for i, _ := range keys {
		result[i] = &result[i]
	}

	//Note: Because we pass in a slice of *interface{}, there is no conversion on the sql side
	e := m.ReadInto(sel, keys, result...)
	if e != nil {
		return nil, e
	}

	/*
		//Cast to types
		for i, _ := range keys {
			switch result[i].(type) {
			case []uint8:
				result[i] = string(result[i].([]uint8))
			}
		}
	*/

	return result, nil
}

func (m *TableManager) Create() (int64, error) {

	if len(m.insertReq) == 0 {
		m.insertReq = m.GetInsertRequest()
	}

	//TODO: return the result as well?
	res, e := m.database.Exec(m.insertReq)
	if e != nil {
		fmt.Printf("SQL REQ FAILED: %s\n", m.insertReq)
		return 0, e
	}

	return res.LastInsertId()
}

func (m *TableManager) Delete(id int64) error {

	if len(m.deleteReq) == 0 {
		m.deleteReq = m.GetDeleteRequest()
	}

	_, e := m.database.Exec(fmt.Sprintf(m.deleteReq, id))
	if e != nil {
		return e
	}
	return nil
}

func (m *TableManager) Close() {
	m.database.Close()
}
