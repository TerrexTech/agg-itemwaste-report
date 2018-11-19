package main

import (
	"encoding/json"
	"log"

	"github.com/TerrexTech/agg-itemwaste-report/report"
	"github.com/TerrexTech/go-eventstore-models/model"
	tlog "github.com/TerrexTech/go-logtransport/log"
	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/TerrexTech/uuuid"
	"github.com/pkg/errors"
)

// Query handles "query" events.
func Query(logger tlog.Logger, itemWasteColl *mongo.Collection, reportColl *mongo.Collection, event *model.Event) *model.KafkaResponse {
	// event.Data should be in this format: `{"timestamp":{"$gt":1529315000},"timestamp":{"$lt":1551997372}}`

	filter := report.WasteItemParams{}

	var reportAgg []report.ReportResult

	err := json.Unmarshal(event.Data, &filter)
	if err != nil {
		err = errors.Wrap(err, "Query: Error while unmarshalling Event-data - ItemWasteReport")
		logger.E(tlog.Entry{
			Description: err.Error(),
			ErrorCode:   1,
		}, filter)
		return &model.KafkaResponse{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     InternalError,
			EventAction:   event.EventAction,
			ServiceAction: event.ServiceAction,
			UUID:          event.UUID,
		}
	}

	if &filter == nil {
		err = errors.New("blank filter provided")
		err = errors.Wrap(err, "Query left blank - ItemWasteReport")
		logger.E(tlog.Entry{
			Description: err.Error(),
			ErrorCode:   1,
		}, filter)
		return &model.KafkaResponse{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     InternalError,
			EventAction:   event.EventAction,
			ServiceAction: event.ServiceAction,
			UUID:          event.UUID,
		}
	}

	avgWasteReport, err := report.ItemWasteReport(filter, itemWasteColl)
	if err != nil {
		err = errors.Wrap(err, "Error getting results from ItemWasteCollection")
		logger.E(tlog.Entry{
			Description: err.Error(),
			ErrorCode:   1,
		}, filter)
		return &model.KafkaResponse{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     InternalError,
			EventAction:   event.EventAction,
			ServiceAction: event.ServiceAction,
			UUID:          event.UUID,
		}
	}

	if len(avgWasteReport) < 1 {
		err = errors.New("Error: No result found from agg_itemwaste collection - Function = ItemWasteReport")
		logger.E(tlog.Entry{
			Description: err.Error(),
			ErrorCode:   1,
		}, reportAgg)
		return &model.KafkaResponse{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     InternalError,
			EventAction:   event.EventAction,
			ServiceAction: event.ServiceAction,
			UUID:          event.UUID,
		}
	}

	for _, v := range avgWasteReport {
		m, assertOK := v.(map[string]interface{})
		if !assertOK {
			err = errors.New("Error getting results from asserting AvgSoldReport into map[string]interface{}")
			logger.E(tlog.Entry{
				Description: err.Error(),
				ErrorCode:   1,
			}, m)
			return &model.KafkaResponse{
				AggregateID:   event.AggregateID,
				CorrelationID: event.CorrelationID,
				Error:         err.Error(),
				ErrorCode:     InternalError,
				EventAction:   event.EventAction,
				ServiceAction: event.ServiceAction,
				UUID:          event.UUID,
			}
		}

		groupByFields := m["_id"]
		mapInGroupBy := groupByFields.(map[string]interface{})
		sku := mapInGroupBy["sku"].(string)
		name := mapInGroupBy["name"].(string)

		// log.Println(m, "#############")

		//if it crashes on wasteWeight - check the wasteWeight field inside db and inside item_waste file--- inside the aggregate pipeline they should match
		reportAgg = append(reportAgg, report.ReportResult{
			SKU:         sku,
			Name:        name,
			WasteWeight: m["avg_waste"].(float64),
			TotalWeight: m["avg_total"].(float64),
		})
	}

	reportID, err := uuuid.NewV4()
	if err != nil {
		err = errors.Wrap(err, "Error in generating reportID ")
		logger.E(tlog.Entry{
			Description: err.Error(),
			ErrorCode:   1,
		})
		return &model.KafkaResponse{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     InternalError,
			EventAction:   event.EventAction,
			ServiceAction: event.ServiceAction,
			UUID:          event.UUID,
		}
	}

	reportGen := report.WasteReport{
		ReportID:     reportID,
		SearchQuery:  filter,
		ReportResult: reportAgg,
	}

	repInsert, err := report.CreateReport(reportGen, reportColl)
	if err != nil {
		err = errors.Wrap(err, "Error in inserting report to mongo")
		logger.E(tlog.Entry{
			Description: err.Error(),
			ErrorCode:   1,
		}, reportGen)
		return &model.KafkaResponse{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     InternalError,
			EventAction:   event.EventAction,
			ServiceAction: event.ServiceAction,
			UUID:          event.UUID,
		}
	}

	log.Println(repInsert)
	// log.Println(reportAgg, "$$$$$$$$$$$$$$$")

	resultMarshal, err := json.Marshal(reportAgg)
	if err != nil {
		err = errors.Wrap(err, "Query: Error marshalling report ItemWasteResults - called reportAgg")
		logger.E(tlog.Entry{
			Description: err.Error(),
			ErrorCode:   1,
		}, reportAgg)
		return &model.KafkaResponse{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     InternalError,
			EventAction:   event.EventAction,
			ServiceAction: event.ServiceAction,
			UUID:          event.UUID,
		}
	}

	return &model.KafkaResponse{
		AggregateID:   event.AggregateID,
		CorrelationID: event.CorrelationID,
		EventAction:   event.EventAction,
		Result:        resultMarshal,
		ServiceAction: event.ServiceAction,
		UUID:          event.UUID,
	}
}
