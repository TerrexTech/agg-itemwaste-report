package report

import (
	util "github.com/TerrexTech/go-commonutils/commonutil"
	"github.com/TerrexTech/uuuid"
	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/bson/objectid"
	"github.com/pkg/errors"
)

type WasteItem struct {
	ID          objectid.ObjectID `bson:"_id,omitempty" json:"_id,omitempty"`
	ItemID      uuuid.UUID        `bson:"itemID,omitempty" json:"itemID,omitempty"`
	WasteID     uuuid.UUID        `bson:"wasteID,omitempty" json:"wasteID,omitempty"`
	SKU         string            `bson:"sku,omitempty" json:"sku,omitempty"`
	Name        string            `bson:"name,omitempty" json:"name,omitempty"`
	Lot         string            `bson:"lot,omitempty" json:"lot,omitempty"`
	Weight      float64           `bson:"weight,omitempty" json:"weight,omitempty"`
	TotalWeight float64           `bson:"totalWeight,omitempty" json:"totalWeight,omitempty"`
	Timestamp   int64             `bson:"timestamp,omitempty" json:"timestamp,omitempty"`
}

type WasteItemParams struct {
	Timestamp *Comparator `json:"timestamp,omitempty"`
}

func (s WasteItem) MarshalBSON() ([]byte, error) {
	si := map[string]interface{}{
		"itemID":      s.ItemID.String(),
		"wasteID":     s.WasteID.String(),
		"lot":         s.Lot,
		"name":        s.Name,
		"sku":         s.SKU,
		"weight":      s.Weight,
		"timestamp":   s.Timestamp,
		"totalWeight": s.TotalWeight,
	}

	if s.ID != objectid.NilObjectID {
		si["_id"] = s.ID
	}
	return bson.Marshal(si)
}

func (s WasteItem) UnmarshalBSON(in []byte) error {
	m := make(map[string]interface{})
	err := bson.Unmarshal(in, m)
	if err != nil {
		err = errors.Wrap(err, "Unmarshal Error")
		return err
	}

	err = s.unmarshalFromMap(m)
	return err
}

func (s WasteItem) unmarshalFromMap(m map[string]interface{}) error {
	var err error
	var assertOK bool

	if m["_id"] != nil {
		s.ID, assertOK = m["_id"].(objectid.ObjectID)
		if !assertOK {
			s.ID, err = objectid.FromHex(m["_id"].(string))
			if err != nil {
				err = errors.Wrap(err, "Error while asserting ObjectID")
				return err
			}
		}
	}

	if m["itemID"] != nil {
		s.ItemID, err = uuuid.FromString(m["itemID"].(string))
		if err != nil {
			err = errors.Wrap(err, "Error while asserting ItemID")
			return err
		}
	}

	if m["wasteID"] != nil {
		s.WasteID, err = uuuid.FromString(m["wasteID"].(string))
		if err != nil {
			err = errors.Wrap(err, "Error while asserting DeviceID")
			return err
		}
	}

	if m["lot"] != nil {
		s.Lot, assertOK = m["lot"].(string)
		if !assertOK {
			return errors.New("Error while asserting Lot")
		}
	}

	if m["name"] != nil {
		s.Name, assertOK = m["name"].(string)
		if !assertOK {
			return errors.New("Error while asserting Name")
		}
	}

	if m["sku"] != nil {
		s.SKU, assertOK = m["sku"].(string)
		if !assertOK {
			return errors.New("Error while asserting Sku")
		}
	}
	if m["weight"] != nil {
		s.Weight, err = util.AssertFloat64(m["weight"])
		if err != nil {
			err = errors.Wrap(err, "Error while asserting Weight")
			return err
		}
	}
	if m["timestamp"] != nil {
		s.Timestamp, err = util.AssertInt64(m["timestamp"])
		if err != nil {
			err = errors.Wrap(err, "Error while asserting Timestamp")
			return err
		}
	}
	if m["totalWeight"] != nil {
		s.TotalWeight, err = util.AssertFloat64(m["totalWeight"])
		if err != nil {
			err = errors.Wrap(err, "Error while asserting TotalWeight")
			return err
		}
	}
	return nil
}
