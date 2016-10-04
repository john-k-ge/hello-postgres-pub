package main

import (
	"database/sql"
	"log"

	"cf-service-tester/cfServiceDiscovery"
	"fmt"
	"os"

	"github.com/cloudfoundry-community/go-cfenv"
	_ "github.com/lib/pq"
)

var pgUrL string
var myService cfServiceDiscovery.ServiceDescriptor

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
			log.Println("No Redis service found!!")
			return
		}

		pgUrL = pgServices[0].Credentials["uri"].(string)
	}

}

func main() {
	if len(pgUrL) > 0 {
		db, err := sql.Open("postgres", pgUrL)
		if err != nil {
			log.Fatal(err)
		}

		rows, err := db.Query("SELECT name FROM public.queryme")
		defer rows.Close()

		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				log.Fatal(err)
			}
			fmt.Printf("The name is %s\n", name)
		}
		if err := rows.Err(); err != nil {
			log.Fatal(err)
		}
	} else {
		fmt.Println("Sorry, no Postgres bound.")
	}

}
