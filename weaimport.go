package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

//OpenWeatherMap struct from api
type OpenWeatherMap struct {
	Coord struct {
		Lon float64 `json:"lon"`
		Lat float64 `json:"lat"`
	} `json:"coord"`
	Weather []struct {
		ID          int    `json:"id"`
		Main        string `json:"main"`
		Description string `json:"description"`
		Icon        string `json:"icon"`
	} `json:"weather"`
	Base string `json:"base"`
	Main struct {
		Temp      float64 `json:"temp"`
		FeelsLike float64 `json:"feels_like"`
		TempMin   float64 `json:"temp_min"`
		TempMax   float64 `json:"temp_max"`
		Pressure  int     `json:"pressure"`
		Humidity  int     `json:"humidity"`
	} `json:"main"`
	Visibility int `json:"visibility"`
	Wind       struct {
		Speed float64 `json:"speed"`
		Deg   int     `json:"deg"`
	} `json:"wind"`
	Clouds struct {
		All int `json:"all"`
	} `json:"clouds"`
	Dt  int `json:"dt"`
	Sys struct {
		Type    int    `json:"type"`
		ID      int    `json:"id"`
		Country string `json:"country"`
		Sunrise int    `json:"sunrise"`
		Sunset  int    `json:"sunset"`
	} `json:"sys"`
	Timezone int    `json:"timezone"`
	ID       int    `json:"id"`
	Name     string `json:"name"`
	Cod      int    `json:"cod"`
}

// weather_api table struct
type wapidbtable struct {
	weather_api_id   int
	weather_api_date string
	temp1            float64
	temp_min         float64
	temp_max         float64
	loc_1            string
	weather_main     string
}

func weatherApiGet(apiKey string, apiLocation string) OpenWeatherMap {
	// func to get JSON data from OpenWeatherMap
	//could modify to take API Key and location as args...
	var url = "https://api.openweathermap.org/data/2.5/weather?q=" + apiLocation + "&APPID=" + apiKey

	res, err := http.Get(url)
	if err != nil {
		log.Fatalf("Error: %s.", err)
		fmt.Println(err)
	}
	//read http body into var body
	body, err := io.ReadAll(res.Body)
	if err != nil {
		fmt.Println(err)
	}

	//JSON Unmarshal Body into result of type OpenWeatherMap
	var result OpenWeatherMap
	if err := json.Unmarshal(body, &result); err != nil { // Parse []byte to go struct pointer
		fmt.Println(err)
	}
	return result
}

//convert from Kelvin to F
func FConvert(Ktemp float64) float64 {
	ftemp := (Ktemp-273.15)*(9/5) + 32
	return ftemp
}

func dbingest(owmResult OpenWeatherMap) {
	//open db connection, defer close
	//edit placeholder with your DB password
	db, err := sql.Open("mysql", "m_user:PLACEHOLDERd@tcp(127.0.0.1:3306)/m_database")
	defer db.Close()

	if err != nil {
		log.Fatal(err)
	}

	//set current time for db ingest
	CurrentTime := time.Now()

	//extracting weather_main
	wm := owmResult.Weather[0].Main

	//convert temp from api to F
	temp1f := FConvert(owmResult.Main.Temp)
	tempminf := FConvert(owmResult.Main.TempMin)
	tempmaxf := FConvert(owmResult.Main.TempMax)

	sql := "INSERT INTO weather_api(weather_api_date, temp1, temp_min, temp_max, loc_1, weather_main ) VALUES (?,?,?,?,?,?)"
	res, err := db.Exec(sql, CurrentTime, temp1f, tempminf, tempmaxf, owmResult.Name, wm)

	if err != nil {
		panic(err.Error())
	}

	res.LastInsertId()

	//logging fact of ingest
	f, err := os.OpenFile("weaimport.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	log.SetOutput(f)
	log.Println("Successful Ingest")
}

func main() {
	//change the values for the below var with your api key and location
	var apiKey = "your_api_key_here"
	var apiLocation = "London"
	//get Weather from API
	owmResultIn := weatherApiGet(apiKey, apiLocation)
	//db ingest Weather
	dbingest(owmResultIn)

}
