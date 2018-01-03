package geohash

import "math"

var (
	// const used to interleave64 and deinterleave64
	// From:  https://graphics.stanford.edu/~seander/bithacks.html#InterleaveBMN
	s = []uint32{0, 1, 2, 4, 8, 16}

	b = []uint64{
		0x5555555555555555,
		0x3333333333333333,
		0x0F0F0F0F0F0F0F0F,
		0x00FF00FF00FF00FF,
		0x0000FFFF0000FFFF,
		0x00000000FFFFFFFF,
	}

	geoalphabet = "0123456789bcdefghjkmnpqrstuvwxyz"
)

const (
	MERCATOR_MAX float64 = 20037726.37

	// Earth's quatratic mean radius for WGS-84
	EARTH_RADIUS_IN_METERS float64 = 6372797.560856

	D_R = (math.Pi / 180.0)
)

func degRad(ang float64) float64 {
	return ang * D_R
}

func radDeg(ang float64) float64 {
	return ang / D_R
}

/* Interleave lower bits of x and y, so the bits of x
 * are in the even positions and bits from y in the odd;
 * x and y must initially be less than 2**32 (65536).
 * From:  https://graphics.stanford.edu/~seander/bithacks.html#InterleaveBMN
 */
func interleave64(xlo uint32, ylo uint32) uint64 {
	var x, y uint64 = uint64(xlo), uint64(ylo)
	x = (x | x<<s[5]) & b[4]
	y = (y | y<<s[5]) & b[4]

	x = (x | x<<s[4]) & b[3]
	y = (y | y<<s[4]) & b[3]

	x = (x | x<<s[3]) & b[2]
	y = (y | y<<s[3]) & b[2]

	x = (x | x<<s[2]) & b[1]
	y = (y | y<<s[2]) & b[1]

	x = (x | x<<s[1]) & b[0]
	y = (y | y<<s[1]) & b[0]

	return x | (y << 1)
}

/* reverse the interleave process
 * derived from http://stackoverflow.com/questions/4909263
 */
func deinterleave64(interleaved uint64) (uint32, uint32) {
	x, y := interleaved, interleaved>>1

	x = (x | (x >> s[0])) & b[0]
	y = (y | (y >> s[0])) & b[0]

	x = (x | (x >> s[1])) & b[1]
	y = (y | (y >> s[1])) & b[1]

	x = (x | (x >> s[2])) & b[2]
	y = (y | (y >> s[2])) & b[2]

	x = (x | (x >> s[3])) & b[3]
	y = (y | (y >> s[3])) & b[3]

	x = (x | (x >> s[4])) & b[4]
	y = (y | (y >> s[4])) & b[4]

	x = (x | (x >> s[5])) & b[5]
	y = (y | (y >> s[5])) & b[5]

	x = x | (y << 32)

	return uint32(x), uint32(x >> 32)
}

func DistBetweenGeoHashWGS84(hash0 uint64, hash1 uint64) float64 {
	lon0d, lat0d := DecodeToLongLatWGS84(hash0)
	lon1d, lat1d := DecodeToLongLatWGS84(hash1)

	return GetDistance(lon0d, lat0d, lon1d, lat1d)
}

// Calculate distance using haversin great circle distance formula.
func GetDistance(lon0d, lat0d, lon1d, lat1d float64) float64 {
	lat0r := degRad(lat0d)
	lon0r := degRad(lon0d)
	lat1r := degRad(lat1d)
	lon1r := degRad(lon1d)

	u := math.Sin((lat1r - lat0r) / 2)
	v := math.Sin((lon1r - lon0r) / 2)

	return 2.0 * EARTH_RADIUS_IN_METERS *
		math.Asin(
			math.Sqrt(
				u*u+
					math.Cos(lat0r)*math.Cos(lat1r)*v*v))
}

func EncodeToBase32(hash uint64) []byte {
	buf := make([]byte, 11)
	var i uint8 = 0
	for ; i < 11; i++ {
		idx := (hash >> (52 - ((i + 1) * 5))) & 0x1f
		buf[i] = geoalphabet[idx]
	}
	return buf
}

func GetAreasByRadiusWGS84(longitude, latitude, radius float64) (*Radius, error) {
	minLon, minLat, maxLon, maxLat := boundingBox(longitude, latitude, radius)

	steps := estimateStepsByRadius(radius, latitude)

	hash, err := Encode(
		WGS84_LONG_RANGE, WGS84_LAT_RANGE,
		longitude, latitude,
		steps)
	if err != nil {
		return nil, err
	}

	neighbors := GetNeighbors(hash)
	area := decode(WGS84_LONG_RANGE, WGS84_LAT_RANGE, hash)

	/* Check if the step is enough at the limits of the covered area.
	 * Sometimes when the search area is near an edge of the
	 * area, the estimated step is not small enough, since one of the
	 * north / south / west / east square is too near to the search area
	 * to cover everything. */
	var decrStep bool

	n := decode(WGS84_LONG_RANGE, WGS84_LAT_RANGE, neighbors.North)
	s := decode(WGS84_LONG_RANGE, WGS84_LAT_RANGE, neighbors.South)
	e := decode(WGS84_LONG_RANGE, WGS84_LAT_RANGE, neighbors.East)
	w := decode(WGS84_LONG_RANGE, WGS84_LAT_RANGE, neighbors.West)

	if GetDistance(longitude, latitude, longitude, n.Latitude.Max) < radius {
		decrStep = true
	}
	if GetDistance(longitude, latitude, longitude, s.Latitude.Min) < radius {
		decrStep = true
	}
	if GetDistance(longitude, latitude, e.Longitude.Max, latitude) < radius {
		decrStep = true
	}
	if GetDistance(longitude, latitude, w.Longitude.Min, latitude) < radius {
		decrStep = true
	}

	if decrStep && steps > 1 {
		steps--
		hash, err = Encode(
			WGS84_LONG_RANGE, WGS84_LAT_RANGE,
			longitude, latitude,
			steps)
		if err != nil {
			return nil, err
		}
		neighbors = GetNeighbors(hash)
		area = decode(WGS84_LONG_RANGE, WGS84_LAT_RANGE, hash)
	}

	/* Exclude the search areas that are useless. */
	if area.Latitude.Min < minLat {
		(&neighbors.South).Clean()
		(&neighbors.SouthWest).Clean()
		(&neighbors.SouthEast).Clean()
	}
	if area.Latitude.Max > maxLat {
		(&neighbors.North).Clean()
		(&neighbors.NorthEast).Clean()
		(&neighbors.NorthWest).Clean()
	}
	if area.Longitude.Min < minLon {
		(&neighbors.West).Clean()
		(&neighbors.SouthWest).Clean()
		(&neighbors.NorthWest).Clean()
	}
	if area.Longitude.Max > maxLon {
		(&neighbors.East).Clean()
		(&neighbors.SouthEast).Clean()
		(&neighbors.NorthEast).Clean()
	}

	return &Radius{
		Area:      *area,
		Hash:      hash,
		Neighbors: neighbors,
	}, nil
}

func boundingBox(longitude, latitude, radius float64) (
	minLongitude float64,
	minLatitude float64,
	maxLongitude float64,
	maxLatitude float64) {

	lonR, latR := degRad(longitude), degRad(latitude)

	if radius > EARTH_RADIUS_IN_METERS {
		radius = EARTH_RADIUS_IN_METERS
	}

	distance := radius / EARTH_RADIUS_IN_METERS
	minLatitude, maxLatitude = latR-distance, latR+distance

	diffLongitude := math.Asin(math.Sin(distance) / math.Cos(latR))

	minLongitude = lonR - diffLongitude
	maxLongitude = lonR + diffLongitude

	minLatitude = radDeg(minLatitude)
	maxLatitude = radDeg(maxLatitude)
	minLongitude = radDeg(minLongitude)
	maxLongitude = radDeg(maxLongitude)

	return
}

/* This function is used in order to estimate the step (bits precision)
 * of the 9 search area boxes during radius queries. */
func estimateStepsByRadius(rangeMeters, latitude float64) uint8 {
	if rangeMeters == 0 {
		return WGS84_GEO_STEP
	}
	var step int8 = 1
	for rangeMeters < MERCATOR_MAX {
		rangeMeters *= 2
		step++
	}

	// Make sure range is included in most of the base cases.
	step -= 2

	// Wider range torwards the poles... Note: it is possible to do better
	// than this approximation by computing the distance between meridians
	// at this latitude, but this does the trick for now.
	if latitude > 66 || latitude < -66 {
		step--
		if latitude > 80 || latitude < -80 {
			step--
		}
	}

	/* Frame to valid range. */
	if step < 1 {
		step = 1
	} else if step > 26 {
		step = 26
	}
	return uint8(step)

}

func GetNeighbors(hash HashBits) *Neighbors {
	neighbors := &Neighbors{
		East:      hash,
		West:      hash,
		North:     hash,
		South:     hash,
		SouthEast: hash,
		SouthWest: hash,
		NorthEast: hash,
		NorthWest: hash,
	}

	moveX(&(neighbors.East), 1)
	moveY(&(neighbors.East), 0)

	moveX(&(neighbors.West), -1)
	moveY(&(neighbors.West), 0)

	moveX(&(neighbors.South), 0)
	moveY(&(neighbors.South), -1)

	moveX(&(neighbors.North), 0)
	moveY(&(neighbors.North), 1)

	moveX(&(neighbors.NorthWest), -1)
	moveY(&(neighbors.NorthWest), 1)

	moveX(&(neighbors.NorthEast), 1)
	moveY(&(neighbors.NorthEast), 1)

	moveX(&(neighbors.SouthEast), 1)
	moveY(&(neighbors.SouthEast), -1)

	moveX(&(neighbors.SouthWest), -1)
	moveY(&(neighbors.SouthWest), -1)

	return neighbors
}

func moveX(hash *HashBits, d int8) *HashBits {
	if d == 0 {
		return hash
	}

	var xmask, ymask uint64 = 0xaaaaaaaaaaaaaaaa, 0x5555555555555555
	var x uint64 = hash.Bits & xmask
	var y uint64 = hash.Bits & ymask

	var zz uint64 = ymask >> (64 - hash.Step*2)

	if d > 0 {
		x = x + (zz + 1)
	} else {
		x = x | zz
		x = x - (zz + 1)
	}

	x &= (xmask >> (64 - hash.Step*2))
	hash.Bits = (x | y)
	return hash
}

func moveY(hash *HashBits, d int8) *HashBits {
	if d == 0 {
		return hash
	}

	var xmask, ymask uint64 = 0xaaaaaaaaaaaaaaaa, 0x5555555555555555

	var x uint64 = hash.Bits & xmask
	var y uint64 = hash.Bits & ymask

	var zz uint64 = xmask >> (64 - hash.Step*2)
	if d > 0 {
		y = y + (zz + 1)
	} else {
		y = y | zz
		y = y - (zz + 1)
	}
	y &= (ymask >> (64 - hash.Step*2))
	hash.Bits = (x | y)
	return hash
}
