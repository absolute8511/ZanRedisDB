package geohash

import (
	"math"
	"testing"
)

func TestWGS84EncodeAndDecode(t *testing.T) {
	type tstruct struct {
		Point
		hash uint64
	}
	places := []tstruct{
		// Tiananmen Square, China
		{Point{116.39772, 39.90323}, 4069885361278926},
		// Arch of Triumph, France
		{Point{2.174266, 48.522679}, 3663813428519937},
		// Chateau de Versailles, France
		{Point{2.71824, 49.481776}, 3664036038846369},
		// Notre Dame de Paris, France
		{Point{2.205695, 48.511139}, 3663813805611339},
		// Louvre, France
		{Point{2.20926, 48.513974}, 3663813812473759},
		// Eiffel Tower Tower, France
		{Point{2.174019, 48.512954}, 3663813411800601},
		// Colosseum in Rome, Italy
		{Point{12.293116, 41.532432}, 3480283575457624},
		// Statue of Liberty, New York City, USA
		{Point{-74.24038, 40.412148}, 1791816099668153},
		// Pyramids, Egypt
		{Point{31.8506, 29.584341}, 3491552924055853},
		// Sphinx, Egypt
		{Point{31.8151, 29.583181}, 3491551447498977},
		// Mount verest
		{Point{86.9221941736, 27.9782502279}, 3639839274149119},
		// Corcovado, The Federative Republic of Brazil
		{Point{-43.123665, -22.57572}, 1008673663509676},
		// Acropolis, The Republic of Greece
		{Point{23.433281, 37.581887}, 3505296982643584},
		// Kilimanjaro, Africa
		{Point{37.205685, -3.35324}, 2670473009705881},
		// Stonehenge, England
		{Point{-1.494338, 51.104432}, 2163357020517338},
		// Sydney Opera House, Australia
		{Point{151.12541, -33.512513}, 3252040564825549},
		// Cologne Cathedral, Germany
		{Point{6.572381, 50.562714}, 3666908289606321},
		// Leaning Tower of Pisa, Italy
		{Point{10.234764, 43.432268}, 3662142917155721},
		// Big Ben, England
		{Point{-0.72796, 51.30266}, 2163508065515980},
		// Buckingham Palace, England
		{Point{-0.83279, 51.30387}, 2163507521029941},
		// Taj Mahal, India
		{Point{78.23188, 27.102839}, 3631332645702463},
	}

	//lonRange := &Range{Min: -180, Max: 180}
	//latRange := &Range{Min: -90, Max: 90}
	for _, v := range places {
		hash, err := EncodeWGS84(v.Longitude, v.Latitude)
		if err != nil {
			t.Fatal(err)
		}
		if hash != v.hash {
			t.Fatalf("the WGS84 geohash of position [%f, %f] should be:%d, not:%d",
				v.Latitude, v.Longitude, v.hash, hash)
		}
	}

	for _, v := range places {
		lon, lat := DecodeToLongLatWGS84(v.hash)
		if math.Abs(lon-v.Longitude) > 0.000003 || math.Abs(lat-v.Latitude) > 0.000003 {
			t.Fatalf("decode WGS84 geohash of position [%f, %f] mismatch, [%f, %f]",
				v.Latitude, v.Longitude, lat, lon)
		}
	}
}

func TestDistance(t *testing.T) {
	type tData struct {
		name string
		lat  float64
		lon  float64
		hash uint64
		dist float64
	}

	center := tData{name: "Tian An Men Square", lat: 39.905637761392, lon: 116.39763057232, hash: 4069885364411786}

	places := []tData{
		{name: "Tian An Men Square", lat: 39.905637761392, lon: 116.39763057232, dist: 0, hash: 4069885364411786},
		{name: "The Great Wall", lat: 40.359759768836, lon: 116.02002181113, dist: 59853.4742, hash: 4069895257856587},
		{name: "The Palace Museum", lat: 39.916345328893, lon: 116.39715582132, dist: 1191.8406, hash: 4069885548623625},
		{name: "The Summer Palace", lat: 39.999886103047, lon: 116.27552270889, dist: 14774.6742, hash: 4069880322548821},
		{name: "Great Hall of the people", lat: 39.9050003, lon: 116.3939423, dist: 322.7538, hash: 4069885362257819},
		{name: "Terracotta Warriors and Horses", lat: 34.384972, lon: 109.274127, dist: 880281.2654, hash: 4040142446455543},
		{name: "West Lake", lat: 30.150197, lon: 120.094491, dist: 1135799.4856, hash: 4054121678641499},
		{name: "Hainan ends of the earth", lon: 109.205175, lat: 18.173128, dist: 2514090.2704, hash: 3974157332439237},
		{name: "Pearl of the Orient", lon: 121.49491, lat: 31.24169, dist: 1067807.3858, hash: 4054803515096369},
		{name: "Buckingham Palace", lon: -0.83279, lat: 51.30387, dist: 8193510.0282, hash: 2163507521029941},
		{name: "Taj Mahal", lon: 78.23188, lat: 27.102839, dist: 3780302.7628, hash: 3631332645702463},
		{name: "Sydney Opera House, Australia", lon: 151.12541, lat: -33.512513, dist: 8912296.5074, hash: 3252040564825549},
		{name: "Pyramids, Egypt", lon: 31.8506, lat: 29.584341, dist: 7525469.5594, hash: 3491552924055853},
		{name: "Statue of Liberty, New York City, USA", lon: -74.24038, lat: 40.412148, dist: 11022442.0136, hash: 1791816099668153},
		{name: "Mount verest", lon: 86.9221941736, lat: 27.9782502279, dist: 3007044.9039, hash: 3639839274149119},
	}

	for _, v := range places {
		dist := GetDistance(center.lon, center.lat, v.lon, v.lat)
		if math.Abs(dist-v.dist) > 0.5 {
			t.Fatalf("distance for Tian An Men Square to %s is %f, not %f", v.name, v.dist, dist)
		}
	}

	for _, v := range places {
		dist := DistBetweenGeoHashWGS84(center.hash, v.hash)
		if math.Abs(dist-v.dist) > 0.0001 {
			t.Fatalf("distance for Tian An Men Square to %s is %f, not %f", v.name, v.dist, dist)
		}
	}

}
