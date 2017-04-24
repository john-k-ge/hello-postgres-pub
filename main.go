package main

import (
	"database/sql"
	"log"

	"github.build.ge.com/212419672/cf-service-tester/cfServiceDiscovery"

	"fmt"
	"os"

	"net/http"

	"encoding/json"

	"errors"

	"strconv"
	"time"

	"strings"

	"github.com/cloudfoundry-community/go-cfenv"
	_ "github.com/lib/pq"
)

var pgUrL string
var myService cfServiceDiscovery.ServiceDescriptor

var createTableQ = "CREATE TABLE public.%s (name   character varying(32))"
var insertQ = "INSERT INTO public.%s(name) VALUES('%s')"
var valueQ = "SELECT NAME from public.%s"
var dropTableQ = "DROP TABLE public.%s"

func simpleQuery(query string) error {
	db, err := sql.Open("postgres", pgUrL)
	defer db.Close()

	if err != nil {
		return errors.New("Cannot connect to DB to create a table: " + err.Error())
	}

	_, err = db.Query(query)
	if err != nil {
		return errors.New("Received an error running " + query + " :: " + err.Error())
	}
	return nil

}

func rowQuery(query string) ([]string, error) {
	var results []string
	db, err := sql.Open("postgres", pgUrL)
	defer db.Close()

	if err != nil {
		return results, errors.New("Cannot connect to DB to create a table: " + err.Error())
	}

	rows, err := db.Query(query)
	if err != nil {
		return results, errors.New("Received an error running " + query + " :: " + err.Error())
	}

	defer rows.Close()
	err = rows.Err()
	if err != nil {
		return results, errors.New("Could not parse resultset: " + err.Error())
	}

	if rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return results, errors.New("Could not parse row value: " + err.Error())
		}
		results = append(results, name)
		fmt.Printf("results now has %v entries!\n", len(results))
	}

	fmt.Printf("Returning: %s\n", results[0])

	return results, nil
}

func dbTest() string {
	tableName := "TABLE_" + strconv.FormatInt(time.Now().Unix(), 10)
	ogVal := "VAL_" + strconv.FormatInt(time.Now().Unix(), 10)

	//Create the table
	err := simpleQuery(fmt.Sprintf(createTableQ, tableName))
	if err != nil {
		fmt.Println("Could not create a table :: " + err.Error())
		return "Could not create a table :: " + err.Error()
	}

	err = simpleQuery(fmt.Sprintf(insertQ, tableName, ogVal))
	if err != nil {
		fmt.Println("Could not insert into table :: " + err.Error())
		return "Could not insert into table :: " + err.Error()
	}

	vals, err := rowQuery(fmt.Sprintf(valueQ, tableName))
	if err != nil {
		fmt.Println("Could not query data :: " + err.Error())
		return "Could not query data :: " + err.Error()
	}
	if len(vals) <= 0 {
		fmt.Println("Something went wrong: I queried the table, but no data was returned.")
		return "Something went wrong: I queried the table, but no data was returned."
	}
	if !strings.EqualFold(ogVal, vals[0]) {
		fmt.Println("Something went wrong: I queried the table, but the data doesn't match.")
		return "Something went wrong: I queried the table, but the data doesn't match."
	}
	err = simpleQuery(fmt.Sprintf(dropTableQ, tableName))
	if err != nil {
		fmt.Println("I could create the table and query fine. I could not drop the table :: " + err.Error())
		return "I could create the table and query fine. I could not drop the table :: " + err.Error()
	}
	fmt.Printf("I successfully created table %v, inserted %v, received %v, and dropped the table.  Everything is fine!", tableName, ogVal, vals[0])

	return fmt.Sprintf("I successfully created table %v, inserted %v, received %v, and dropped the table.  Everything is fine!", tableName, ogVal, vals[0])

}

func handleDBTest(w http.ResponseWriter, req *http.Request) {
	fmt.Printf("PGUrl: %s\n", pgUrL)
	if len(pgUrL) <= 0 {
		fmt.Println("I'm not bound to a Postgres instance!  Please bind me!")
		fmt.Fprintf(w, "I'm not bound to a Postgres instance!  Please bind me!\n")
		return
	}

	fmt.Fprintf(w, dbTest())
}

// Return my service descriptor metadata
func serviceDescriptor(w http.ResponseWriter, req *http.Request) {
	data, err := json.Marshal(&myService)
	if err != nil {
		fmt.Printf("Cannot generate service descriptor: %v", err)
		fmt.Fprintf(w, "Cannot generate service descriptor: %v", err)
		return
	}
	fmt.Printf("Here's the data:  %s\n", data)
	//fmt.Fprintf(w, "%s", myService)
	json.NewEncoder(w).Encode(myService)
}

func init() {
	appEnv, _ := cfenv.Current()

	myService = cfServiceDiscovery.ServiceDescriptor{
		AppName:     appEnv.Name,
		AppUri:      appEnv.ApplicationURIs[0],
		ServiceName: os.Getenv("SERVICE_NAME"),
		PlanName:    os.Getenv("SERVICE_PLAN"),
	}

	services := appEnv.Services
	if len(services) > 0 {
		fmt.Printf("RDPG ServiceTag = %v\n", myService.ServiceName)
		pgServices, err := services.WithLabel(myService.ServiceName)

		if err != nil || len(pgServices) <= 0 {
			log.Println("No Postgres service found!!")
			return
		}

		pgUrL = pgServices[0].Credentials["uri"].(string)
	}

}

func main() {
	fmt.Println("Starting...")
	port := os.Getenv("PORT")
	if len(port) == 0 {
		port = "9000"
	}
	log.Printf("Listening on port %v", port)

	//dbCall()
	http.HandleFunc("/info", serviceDescriptor)
	http.HandleFunc("/ping", handleDBTest)
	err := http.ListenAndServe(":"+port, nil)
	if err != nil {
		log.Printf("ListenAndServe: %v", err)
	}
}
