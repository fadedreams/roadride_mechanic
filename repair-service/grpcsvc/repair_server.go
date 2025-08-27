package grpcsvc

import (
    "log"
    "repair-service/domain"
    "repair-service/proto"

    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/attribute"
    "go.opentelemetry.io/otel/codes"
)

type RepairServer struct {
    proto.UnimplementedRepairServiceServer
    repo domain.RepairRepository
}

func NewRepairServer(repo domain.RepairRepository) *RepairServer {
    return &RepairServer{repo: repo}
}

func (s *RepairServer) StreamAllRepairs(_ *proto.Empty, stream proto.RepairService_StreamAllRepairsServer) error {
    ctx, span := otel.Tracer("repair-service").Start(stream.Context(), "StreamAllRepairs")
    defer span.End()

    // Get all existing repairs
    repairs, err := s.repo.GetAllRepairs(ctx)
    if err != nil {
        span.RecordError(err)
        span.SetStatus(codes.Error, "Failed to get initial repairs")
        log.Printf("Failed to get initial repairs: %v", err)
        return err
    }

    // Send initial repairs
    for _, repair := range repairs {
        protoRepair := convertToProtoRepair(repair)
        if err := stream.Send(protoRepair); err != nil {
            span.RecordError(err)
            span.SetStatus(codes.Error, "Failed to send repair")
            log.Printf("Failed to send repair: %v", err)
            return err
        }
    }
    span.SetAttributes(attribute.Int("initialRepairCount", len(repairs)))

    // Set up MongoDB change stream to watch for new repairs
    changeStream, err := s.repo.WatchRepairs(ctx)
    if err != nil {
        span.RecordError(err)
        span.SetStatus(codes.Error, "Failed to open change stream")
        log.Printf("Failed to open change stream: %v", err)
        return err
    }
    defer changeStream.Close(ctx)

    // Stream new repairs
    for changeStream.Next(ctx) {
        var changeDoc struct {
            FullDocument domain.RepairModel `bson:"fullDocument"`
        }
        if err := changeStream.Decode(&changeDoc); err != nil {
            span.RecordError(err)
            span.SetStatus(codes.Error, "Failed to decode change stream document")
            log.Printf("Failed to decode change stream document: %v", err)
            return err
        }

        protoRepair := convertToProtoRepair(&changeDoc.FullDocument)
        if err := stream.Send(protoRepair); err != nil {
            span.RecordError(err)
            span.SetStatus(codes.Error, "Failed to send new repair")
            log.Printf("Failed to send new repair: %v", err)
            return err
        }
        span.SetAttributes(attribute.String("newRepairID", protoRepair.Id))
        log.Printf("Streamed new repair: %s", protoRepair.Id)
    }

    if err := changeStream.Err(); err != nil {
        span.RecordError(err)
        span.SetStatus(codes.Error, "Change stream error")
        log.Printf("Change stream error: %v", err)
        return err
    }

    return nil
}

// convertToProtoRepair converts domain.RepairModel to proto.Repair
func convertToProtoRepair(repair *domain.RepairModel) *proto.Repair {
    if repair == nil || repair.RepairCost == nil {
        return &proto.Repair{
            Id:      repair.ID,
            UserId:  repair.UserID,
            Status:  repair.Status,
        }
    }

    protoMechanics := make([]*proto.MechanicInfo, len(repair.RepairCost.Mechanics))
    for i, m := range repair.RepairCost.Mechanics {
        protoMechanics[i] = &proto.MechanicInfo{
            Id:       m.ID,
            Name:     m.Name,
            Location: &proto.Location{Longitude: m.Location.Longitude, Latitude: m.Location.Latitude},
            Distance: m.Distance,
        }
    }

    var userLocation *proto.Location
    if repair.RepairCost.UserLocation != nil {
        userLocation = &proto.Location{
            Longitude: repair.RepairCost.UserLocation.Longitude,
            Latitude:  repair.RepairCost.UserLocation.Latitude,
        }
    }

    return &proto.Repair{
        Id:      repair.ID,
        UserId:  repair.UserID,
        Status:  repair.Status,
        RepairCost: &proto.RepairCost{
            Id:         repair.RepairCost.ID,
            UserId:     repair.RepairCost.UserID,
            RepairType: repair.RepairCost.RepairType,
            TotalPrice: repair.RepairCost.TotalPrice,
            UserLocation: userLocation,
            Mechanics:  protoMechanics,
        },
    }
}
