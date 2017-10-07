package geohash

type Range struct {
	Max float64
	Min float64
}

type Point struct {
	Longitude float64
	Latitude  float64
}

type HashBits struct {
	Bits uint64
	Step uint8
}

func (hash HashBits) IsZero() bool {
	return hash.Bits == 0 && hash.Step == 0
}

func (hash *HashBits) Clean() {
	hash.Bits = 0
	hash.Step = 0
}

type Neighbors struct {
	North     HashBits
	East      HashBits
	West      HashBits
	South     HashBits
	NorthEast HashBits
	SouthEast HashBits
	NorthWest HashBits
	SouthWest HashBits
}

type Area struct {
	Hash      HashBits
	Longitude Range
	Latitude  Range
}

type Radius struct {
	Area
	Hash HashBits
	*Neighbors
}
