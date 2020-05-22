package googlespreadsheet

import (
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"

	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/sheets/v4"
)

func googleAuth(confData []byte) (*http.Client, error) {

	conf, err := google.JWTConfigFromJSON(confData, sheets.SpreadsheetsScope)
	if err != nil {
		return nil, err
	}
	return conf.Client(context.TODO()), nil
}

//Config represents auth and spreadsheet info to access google spreadsheet
type Config struct {
	GoogleCredentials []byte
	SpreadsheetID     string
	Client            *http.Client
}

//ColAddress returns a column letter (like "A" or "AA") corresponding to an int.
//if int <=0 or >675 returns ""
func ColAddress(col int) string {
	if col >= 675 || col < 1 {
		return ""
	}
	if col <= 26 {
		return string(int('A') + col - 1)
	}
	return string(int('A')+int(col/26)-1) + string(int('A')+(col%26)-1)
}

//ClearRange clears a destination range ( sheetname!A1:B34 )
func ClearRange(googleConf *Config, theRange string) error {
	var err error
	if googleConf.Client == nil { //not authorized yet
		googleConf.Client, err = googleAuth(googleConf.GoogleCredentials)
		if err != nil {
			return err
		}
	}
	//construct the update call
	srv, err := sheets.New(googleConf.Client)
	if err != nil {
		return err
	}

	values := srv.Spreadsheets.Values
	clear := sheets.ClearValuesRequest{}
	clearCall := values.Clear(googleConf.SpreadsheetID, theRange, &clear)
	_, err = clearCall.Do()
	return err
}

//DataMapToGoogleSpreadsheet transfer a []map[string]interface{} array to a google spreadsheet
func DataMapToGoogleSpreadsheet(googleConf *Config, sheet string, row int, col int, data []map[string]interface{}) error {
	//calculate destination range
	nbRows := len(data)
	if nbRows == 0 {
		return nil
	}
	nbCols := len(data[0])
	if nbCols == 0 {
		return nil
	}

	//prepare an array with all the data
	keys := make([]string, nbCols)
	i := 0
	for k := range data[0] {
		keys[i] = k
		i++
	}
	sort.Strings(keys)
	valueData := make([][]interface{}, nbRows+1) // +1 for header row
	valueData[0] = make([]interface{}, nbCols)

	//first line with headers

	for k, v := range keys {
		valueData[0][k] = v
	}

	//next line with data
	for row, rowvalue := range data {
		valueData[row+1] = make([]interface{}, nbCols)
		for col, k := range keys {
			var str sql.NullString
			str.Scan(rowvalue[k])
			valueData[row+1][col] = str.String
		}
	}

	return DataArrayToGoogleSpreadSheet(googleConf, sheet, row, col, valueData)
}

//DataArrayToGoogleSpreadSheet transfer a [][]interface{} array to a google spreadsheet
func DataArrayToGoogleSpreadSheet(googleConf *Config, destSheet string, destRow int, destCol int, data [][]interface{}) error {
	var err error
	//calculate destination range
	nbRows := len(data)
	if nbRows == 0 {
		return nil
	}
	nbCols := len(data[0])
	if nbCols == 0 {
		return nil
	}
	myRange := destSheet + "!" +
		ColAddress(destCol) + strconv.Itoa(destRow) +
		":" +
		ColAddress(destCol+nbCols) + strconv.Itoa(destRow+nbRows)

	//prepare data for spreadsheet insertion
	valueRange := sheets.ValueRange{
		MajorDimension: "ROWS",
		Values:         data}

	//check google auth
	if googleConf.Client == nil { //not authorized yet
		googleConf.Client, err = googleAuth(googleConf.GoogleCredentials)
		if err != nil {
			return err
		}
	}

	//construct the update call
	srv, err := sheets.New(googleConf.Client)
	spreadsheets := srv.Spreadsheets
	values := spreadsheets.Values

	updateCall := values.Update(googleConf.SpreadsheetID, myRange, &valueRange)
	updateCall.ValueInputOption("USER_ENTERED")

	//send the update call request
	updateResponse, err := updateCall.Do()
	if err != nil {
		return err
	}

	if updateResponse.ServerResponse.HTTPStatusCode < 200 || updateResponse.ServerResponse.HTTPStatusCode > 299 {
		return fmt.Errorf("Wrong http return code %d ", updateResponse.ServerResponse.HTTPStatusCode)
	}
	return nil
}

//GoogleSpreadsheetToDataArray transfer  a google spreadsheet to  a [][]interface{} array
func GoogleSpreadsheetToDataArray(googleConf *Config, sourceRange string) ([][]interface{}, error) {
	var err error
	//check google auth
	if googleConf.Client == nil { //not authorized yet
		googleConf.Client, err = googleAuth(googleConf.GoogleCredentials)
		if err != nil {
			return nil, err
		}
	}
	//construct the update call
	sheetsService, err := sheets.New(googleConf.Client)

	//read values from spreadhsset :
	result, err := sheetsService.Spreadsheets.Values.Get(googleConf.SpreadsheetID, sourceRange).Do()
	if err != nil {
		fmt.Printf("ERROR received on Google Spreadsheet request : " + err.Error())
		return nil, err
	}

	if len(result.Values) == 0 {
		fmt.Printf("No values received\n")
		return nil, errors.New("Empty template")
	}
	return result.Values, nil
}

//ClearSheet clear values
func ClearSheet(googleConf *Config, sourceRange string) error {
	var err error
	//check google auth
	if googleConf.Client == nil { //not authorized yet
		googleConf.Client, err = googleAuth(googleConf.GoogleCredentials)
		if err != nil {
			return err
		}
	}

	sheetsService, err := sheets.New(googleConf.Client)
	//construct the clear call
	rb := &sheets.ClearValuesRequest{}
	_, err = sheetsService.Spreadsheets.Values.Clear(googleConf.SpreadsheetID, sourceRange, rb).Do()

	if err != nil {
		fmt.Printf("ERROR received on Google Spreadsheet request : " + err.Error())
		return err
	}

	return nil
}
