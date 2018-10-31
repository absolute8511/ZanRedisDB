// Derived from the C code implementation of Redis, http://redis.io
// Original copyright states...

package node

import (
	"errors"
	"sort"
	"strconv"
	"strings"

	"github.com/absolute8511/redcon"
	"github.com/youzan/ZanRedisDB/common"
	"github.com/youzan/ZanRedisDB/common/geohash"
)

var (
	ifGeoHashUnitTest = false
)

type searchType int

const (
	RADIUS_COORDS searchType = iota
	RADIUS_MEMBER
)

type sortType int

const (
	SORT_NONE sortType = iota
	SORT_ASC
	SORT_DESC
)

/* usage:
GEOADD key lon0 lat0 elem0 lon1 lat1 elem1
*/
func (nd *KVNode) geoaddCommand(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) < 5 || (len(cmd.Args)-2)%3 != 0 {
		conn.WriteError("ERR wrong number of arguments for 'geoadd' command")
		return
	}

	var zaddCmd redcon.Command
	zaddCmd.Args = make([][]byte, (len(cmd.Args)-2)/3*2+2)

	zaddCmd.Args[0] = []byte("zadd")
	zaddCmd.Args[1] = cmd.Args[1]

	for i := 0; i < (len(cmd.Args)-2)/3; i++ {
		lon, err := strconv.ParseFloat(string(cmd.Args[i*3+2]), 64)
		if err != nil {
			conn.WriteError("ERR value is not a valid float")
			return
		}
		lat, err := strconv.ParseFloat(string(cmd.Args[i*3+3]), 64)
		if err != nil {
			conn.WriteError("ERR value is not a valid float")
			return
		}

		hash, err := geohash.EncodeWGS84(lon, lat)
		if err != nil {
			conn.WriteError("Err " + err.Error())
			return
		}

		zaddCmd.Args[i*2+2] = strconv.AppendUint(zaddCmd.Args[i*2+2], hash, 10)
		zaddCmd.Args[i*2+3] = cmd.Args[i*3+4]
	}

	if ifGeoHashUnitTest {
		/* The code used for unit test. */
		_, key, err := common.ExtractNamesapce(cmd.Args[1])
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		if common.IsValidTableName(key) {
			conn.WriteError(common.ErrInvalidTableName.Error())
			return
		}
		zaddCmd.Args[1] = key
		sm, ok := nd.sm.(*kvStoreSM)
		if !ok {
			conn.WriteError("Err not supported state machine")
			return
		}
		if _, err := sm.localZaddCommand(buildCommand(zaddCmd.Args), -1); err != nil {
			conn.WriteError("Err " + err.Error())
		}

	} else {
		/* The code actually execute. */
		nd.zaddCommand(conn, buildCommand(zaddCmd.Args))
	}
}

/* usage:
GEODIST key elem0 elem1 [unit]
*/
func (nd *KVNode) geodistCommand(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 4 && len(cmd.Args) != 5 {
		conn.WriteError("ERR wrong number of arguments for 'geodist' command")
		return
	}

	var toMeters float64 = 1.0
	var err error

	if len(cmd.Args) == 5 {
		toMeters, err = extractUnit(cmd.Args[4])
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
	}

	hash0, err := nd.store.ZScore(cmd.Args[1], cmd.Args[2])
	if err != nil {
		conn.WriteNull()
		return
	}

	hash1, err := nd.store.ZScore(cmd.Args[1], cmd.Args[3])
	if err != nil {
		conn.WriteNull()
		return
	}

	distance := geohash.DistBetweenGeoHashWGS84(uint64(hash0), uint64(hash1)) / toMeters

	conn.WriteBulk([]byte(strconv.FormatFloat(distance, 'g', -1, 64)))
}

/* usage:
GEOHASH key elem0 elem1 ... elemN
*/
func (nd *KVNode) geohashCommand(conn redcon.Conn, cmd redcon.Command) {
	conn.WriteArray(len(cmd.Args) - 2)

	for _, member := range cmd.Args[2:] {
		hash, err := nd.store.ZScore(cmd.Args[1], member)
		if err != nil {
			//conn.WriteString(err.Error())
			conn.WriteNull()
		} else {
			/* The internal format we use for geocoding is a bit different
			 * than the standard, since we use as initial latitude range
			 * -85,85, while the normal geohashing algorithm uses -90,90.
			 * So we have to decode our position and re-encode using the
			 * standard ranges in order to output a valid geohash string. */
			/* Decode... */
			longitude, latitude := geohash.DecodeToLongLatWGS84(uint64(hash))
			code, _ := geohash.Encode(
				&geohash.Range{Max: 180, Min: -180},
				&geohash.Range{Max: 90, Min: -90},
				longitude,
				latitude,
				geohash.WGS84_GEO_STEP)
			conn.WriteBulk(geohash.EncodeToBase32(code.Bits))
		}
	}
}

/* usage:
GEOPOS key elem0 elem1 ... elemN
*/
func (nd *KVNode) geoposCommand(conn redcon.Conn, cmd redcon.Command) {
	conn.WriteArray(len(cmd.Args) - 2)

	for _, member := range cmd.Args[2:] {
		hash, err := nd.store.ZScore(cmd.Args[1], member)
		if err != nil {
			conn.WriteNull()
		} else {
			longitude, latitude := geohash.DecodeToLongLatWGS84(uint64(hash))
			conn.WriteArray(2)
			conn.WriteBulk([]byte(strconv.FormatFloat(longitude, 'g', -1, 64)))
			conn.WriteBulk([]byte(strconv.FormatFloat(latitude, 'g', -1, 64)))
		}
	}
}

/* usage:
GEORADIUS key longitude latitude radius m|km|ft|mi [WITHCOORD] [WITHDIST]
[WITHHASH] [COUNT count] [ASC|DESC] [STORE key] [STOREDIST key]
*/
func (nd *KVNode) geoRadiusCommand(conn redcon.Conn, cmd redcon.Command) {
	nd.geoRadiusGeneric(conn, cmd, RADIUS_COORDS)
}

/* usage:
GEORADIUSBYMEMBER key member radius m|km|ft|mi [WITHCOORD] [WITHDIST]
[WITHHASH] [COUNT count] [ASC|DESC] [STORE key] [STOREDIST key]
*/
func (nd *KVNode) geoRadiusByMemberCommand(conn redcon.Conn, cmd redcon.Command) {
	nd.geoRadiusGeneric(conn, cmd, RADIUS_MEMBER)
}

func (nd *KVNode) geoRadiusGeneric(conn redcon.Conn, cmd redcon.Command, stype searchType) {
	var x, y float64
	var err error

	if card, err := nd.store.ZCard(cmd.Args[1]); err != nil || card == 0 {
		if err != nil {
			conn.WriteError(err.Error())
		} else {
			conn.WriteArray(0)
		}
		return
	}

	var baseArgs = 0

	switch stype {
	case RADIUS_COORDS:
		baseArgs = 4
		if x, err = strconv.ParseFloat(string(cmd.Args[2]), 64); err != nil {
			conn.WriteError("Err value is not a valid float")
			return
		}
		if y, err = strconv.ParseFloat(string(cmd.Args[3]), 64); err != nil {
			conn.WriteError("Err value is not a valid float")
			return
		}

	case RADIUS_MEMBER:
		baseArgs = 3
		hash, err := nd.store.ZScore(cmd.Args[1], cmd.Args[2])
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		x, y = geohash.DecodeToLongLatWGS84(uint64(hash))

	default:
		conn.WriteError("unknown georadius search type")
		return
	}

	radiusMeters, conversion, err := extractDistance(cmd.Args[baseArgs], cmd.Args[baseArgs+1])
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	var withdist, withhash, withcoords bool
	var sortT sortType
	var optLen, count int

	/* Parse the radius search opts.*/
	opts := cmd.Args[baseArgs+2:]
	for i := 0; i < len(opts); i++ {
		option := strings.ToLower(string(opts[i]))
		switch option {
		case "withdist":
			withdist = true
			optLen++
		case "withcoord":
			withcoords = true
			optLen++
		case "withhash":
			withhash = true
			optLen++
		case "asc":
			sortT = SORT_ASC
		case "desc":
			sortT = SORT_DESC
		case "count":
			if i+1 >= len(opts) {
				err = errors.New("ERR syntax error")
			} else {
				count, err = strconv.Atoi(string(opts[i+1]))
				if err != nil {
					err = errors.New("ERR value is not an integer or out of range")
				} else if count < 0 {
					err = errors.New("ERR COUNT must > 0")
				} else {
					i++
				}
			}
		default:
			err = errors.New("ERR syntax error")
		}
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
	}

	/* COUNT without ordering does not make much sense, force ASC
	 * ordering if COUNT was specified but no sorting was requested. */
	if count != 0 && sortT == SORT_NONE {
		sortT = SORT_ASC
	}

	radiusArea, err := geohash.GetAreasByRadiusWGS84(x, y, radiusMeters)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	plist, err := nd.geoMembersOfAllNeighbors(cmd.Args[1], radiusArea, x, y, radiusMeters)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	if count == 0 || len(plist) < count {
		count = len(plist)
	}

	/* Sort the returned geoPoints. */
	switch sortT {
	case SORT_ASC:
		slice := geoPointsSlice(plist)
		sort.Sort(slice)
	case SORT_DESC:
		slice := geoPointsSlice(plist)
		sort.Sort(sort.Reverse(slice))
	default:
	}

	/* Return results to user. */
	conn.WriteArray(count)

	for _, point := range plist[:count] {
		if optLen > 0 {
			conn.WriteArray(optLen + 1)
		}

		conn.WriteBulk(point.member)

		if withdist {
			dist := point.dist / conversion
			conn.WriteBulk([]byte(strconv.FormatFloat(dist, 'g', -1, 64)))
		}

		if withhash {
			conn.WriteInt64(int64(point.score))
		}

		if withcoords {
			conn.WriteArray(2)
			conn.WriteBulk([]byte(strconv.FormatFloat(point.longitude, 'g', -1, 64)))
			conn.WriteBulk([]byte(strconv.FormatFloat(point.latitude, 'g', -1, 64)))
		}
	}
}

func extractUnit(unit []byte) (float64, error) {
	switch string(unit) {
	case "m":
		return 1, nil
	case "km":
		return 1000, nil
	case "ft":
		return 0.3048, nil
	case "mi":
		return 1609.34, nil
	default:
		return -1, errors.New("Unsupported unit provided. please use m, km, ft, mi")
	}
}

func extractDistance(radius []byte, unit []byte) (float64, float64, error) {
	distance, err := strconv.ParseFloat(string(radius), 64)
	if err != nil {
		return -1, -1, errors.New("need numeric radius")
	} else if distance < 0 {
		return -1, -1, errors.New("radius cannot be negative")
	}

	toMeters, err := extractUnit(unit)
	if err != nil {
		return -1, -1, err
	}

	return distance * toMeters, toMeters, nil
}

func (nd *KVNode) geoMembersOfAllNeighbors(set []byte, geoRadius *geohash.Radius, lon, lat, radius float64) ([]*geoPoints, error) {
	neighbors := [9]*geohash.HashBits{
		&geoRadius.Hash,
		&geoRadius.North,
		&geoRadius.South,
		&geoRadius.East,
		&geoRadius.West,
		&geoRadius.NorthEast,
		&geoRadius.NorthWest,
		&geoRadius.SouthEast,
		&geoRadius.SouthWest,
	}

	var lastProcessed int = 0
	plist := make([]*geoPoints, 0, 64)

	for i, area := range neighbors {
		if area.IsZero() {
			continue
		}
		// When a huge Radius (in the 5000 km range or more) is used,
		// adjacent neighbors can be the same, leading to duplicated
		// elements. Skip every range which is the same as the one
		// processed previously.
		if lastProcessed != 0 &&
			area.Bits == neighbors[lastProcessed].Bits &&
			area.Step == neighbors[lastProcessed].Step {
			continue
		}
		ps, err := nd.membersOfGeoHashBox(set, lon, lat, radius, area)
		if err != nil {
			return nil, err
		} else {
			plist = append(plist, ps...)
		}
		lastProcessed = i
	}
	return plist, nil
}

type geoPoints struct {
	longitude float64
	latitude  float64
	dist      float64
	score     float64
	member    []byte
}

// Obtain all members between the min/max of this geohash bounding box.
func (nd *KVNode) membersOfGeoHashBox(zset []byte, longitude, latitude, radius float64, hash *geohash.HashBits) ([]*geoPoints, error) {
	points := make([]*geoPoints, 0, 32)
	min, max := scoresOfGeoHashBox(hash)
	vlist, err := nd.store.ZRangeByScoreGeneric(zset, float64(min), float64(max), 0, -1, false)
	if err != nil {
		return nil, err
	}

	for _, v := range vlist {
		x, y := geohash.DecodeToLongLatWGS84(uint64(v.Score))
		dist := geohash.GetDistance(x, y, longitude, latitude)
		if radius >= dist {
			p := &geoPoints{
				longitude: x,
				latitude:  y,
				dist:      dist,
				score:     v.Score,
				member:    v.Member,
			}
			points = append(points, p)
		}
	}

	return points, nil
}

/* Compute the sorted set scores min (inclusive), max (exclusive) we should
 * query in order to retrieve all the elements inside the specified area
 * 'hash'. The two scores are returned by reference in *min and *max. */
func scoresOfGeoHashBox(hash *geohash.HashBits) (min, max uint64) {
	min = hash.Bits << (geohash.WGS84_GEO_STEP*2 - hash.Step*2)
	bits := hash.Bits + 1
	max = bits << (geohash.WGS84_GEO_STEP*2 - hash.Step*2)
	return
}

type geoPointsSlice []*geoPoints

func (slice geoPointsSlice) Less(i, j int) bool {
	return slice[i].dist < slice[j].dist
}

func (slice geoPointsSlice) Len() int {
	return len(slice)
}

func (slice geoPointsSlice) Swap(i, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}
