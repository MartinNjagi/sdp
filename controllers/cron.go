package controllers

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

// StartCampaignMonitor launches a 10-minute ticker to sweep for completed or expired campaigns.
func StartCampaignMonitor(ctx context.Context, db *gorm.DB) {
	ticker := time.NewTicker(10 * time.Minute)

	go func() {
		logrus.Info("[CampaignCron] Started 10-minute monitor loop (24h TTL envelope) ✅")
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				logrus.Info("[CampaignCron] Stopping monitor...")
				return
			case <-ticker.C:
				sweepCampaigns(db)
			}
		}
	}()
}

func sweepCampaigns(db *gorm.DB) {
	// 1. NORMAL COMPLETION:
	// Look for 'PROCESSED' campaigns (fan-out done) where all outboxes are terminal.
	queryNormal := `
		UPDATE campaigns c
		SET c.status = 'COMPLETED', c.updated_at = NOW()
		WHERE c.status = 'PROCESSED' 
		AND NOT EXISTS (
			SELECT 1 FROM outboxes o 
			WHERE o.campaign_id = c.id 
			AND o.status IN ('PENDING', 'SENT')
		)`

	resNormal := db.Exec(queryNormal)
	if resNormal.Error != nil {
		logrus.Errorf("[CampaignCron] Failed to sweep normal completions: %v", resNormal.Error)
	} else if resNormal.RowsAffected > 0 {
		logrus.Infof("[CampaignCron] Marked %d campaigns as COMPLETED", resNormal.RowsAffected)
	}

	// 2. TIMEOUT ENVELOPE (24 HOURS):
	// Catch ANY active campaigns ('PROCESSING' or 'PROCESSED') that are older than 24 hours.
	queryTimeout := `
		UPDATE campaigns c
		SET c.status = 'COMPLETED', c.updated_at = NOW()
		WHERE c.status IN ('PROCESSING', 'PROCESSED') 
		AND c.updated_at < NOW() - INTERVAL 24 HOUR`

	resTimeout := db.Exec(queryTimeout)
	if resTimeout.Error != nil {
		logrus.Errorf("[CampaignCron] Failed to sweep 24h timeouts: %v", resTimeout.Error)
	} else if resTimeout.RowsAffected > 0 {
		logrus.Warnf("[CampaignCron] Force-closed %d campaigns as COMPLETED (Hit 24-hour TTL)", resTimeout.RowsAffected)
	}
}
