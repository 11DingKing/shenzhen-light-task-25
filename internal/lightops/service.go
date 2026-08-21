package lightops

import (
	"context"
	"errors"
	"fmt"
	"slices"
)

type Service struct{ store *Store }

func NewService(store *Store) *Service {
	if store == nil {
		store = NewStore()
	}
	return &Service{store: store}
}

func complaintKey(district, key string) string      { return district + ":complaint:" + key }
func actionKey(district, action, key string) string { return district + ":" + action + ":" + key }

func (s *Service) SubmitComplaint(ctx context.Context, complaint Complaint, key string) (Complaint, error) {
	if err := ctx.Err(); err != nil {
		return Complaint{}, err
	}
	if complaint.ID == "" || complaint.DistrictID == "" || len(complaint.Evidence) == 0 {
		return Complaint{}, ErrConflict
	}
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	if prior, ok := s.store.idempotency[complaintKey(complaint.DistrictID, key)]; ok {
		return cloneComplaint(s.store.complaints[prior.EntityID]), nil
	}
	if _, ok := s.store.complaints[complaint.ID]; ok {
		return Complaint{}, ErrConflict
	}
	complaint.Status, complaint.Version, complaint.Evidence = "pending", 1, slices.Clone(complaint.Evidence)
	s.store.complaints[complaint.ID] = complaint
	if err := s.store.appendEventLocked(Event{ID: complaint.ID + "-submitted", DistrictID: complaint.DistrictID, EntityID: complaint.ID, Action: "complaint.submitted", Metadata: map[string]string{"resident": complaint.ResidentID}}); err != nil {
		delete(s.store.complaints, complaint.ID)
		return Complaint{}, err
	}
	s.store.idempotency[complaintKey(complaint.DistrictID, key)] = OperationResult{EntityID: complaint.ID, Version: complaint.Version}
	return cloneComplaint(complaint), nil
}

func (s *Service) ListComplaints(ctx context.Context, district string) ([]Complaint, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	output := make([]Complaint, 0)
	for _, complaint := range s.store.complaints {
		if complaint.DistrictID == district {
			output = append(output, cloneComplaint(complaint))
		}
	}
	return output, nil
}

func (s *Service) PublishPlan(ctx context.Context, plan RectificationPlan, key string) (RectificationPlan, error) {
	if err := ctx.Err(); err != nil {
		return RectificationPlan{}, err
	}
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	complaint, ok := s.store.complaints[plan.ComplaintID]
	if !ok {
		return RectificationPlan{}, ErrNotFound
	}
	if complaint.DistrictID != plan.DistrictID {
		return RectificationPlan{}, ErrForbidden
	}
	if prior, ok := s.store.idempotency[actionKey(plan.DistrictID, "plan", key)]; ok {
		return clonePlan(s.store.plans[prior.EntityID]), nil
	}
	plan.Status, plan.Version, plan.Steps = "submitted", 1, slices.Clone(plan.Steps)
	if err := s.store.savePlanLocked(plan); err != nil {
		return RectificationPlan{}, err
	}
	s.store.idempotency[actionKey(plan.DistrictID, "plan", key)] = OperationResult{EntityID: plan.ID, Version: plan.Version}
	return clonePlan(plan), nil
}

func (s *Service) RegisterSchedule(ctx context.Context, schedule Schedule) (Schedule, error) {
	if err := ctx.Err(); err != nil {
		return Schedule{}, err
	}
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	schedule.Rows, schedule.Version = cloneRows(schedule.Rows), 1
	s.store.schedules[schedule.ID] = schedule
	return cloneSchedule(schedule), nil
}

func (s *Service) SaveAssessment(ctx context.Context, assessment Assessment) (Assessment, error) {
	if err := ctx.Err(); err != nil {
		return Assessment{}, err
	}
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	complaint, ok := s.store.complaints[assessment.ComplaintID]
	if !ok {
		return Assessment{}, ErrNotFound
	}
	if complaint.DistrictID != assessment.DistrictID {
		return Assessment{}, ErrForbidden
	}
	assessment.Readings, assessment.Version = slices.Clone(assessment.Readings), 1
	s.store.assessments[assessment.ID] = assessment
	complaint.AssessmentID, complaint.Version = assessment.ID, complaint.Version+1
	s.store.complaints[complaint.ID] = complaint
	return cloneAssessment(assessment), nil
}

func (s *Service) AssignZone(ctx context.Context, district, complaintID, zoneID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	complaint, ok := s.store.complaints[complaintID]
	if !ok {
		return ErrNotFound
	}
	if complaint.DistrictID != district {
		return ErrForbidden
	}
	complaint.ZoneID, complaint.Version = zoneID, complaint.Version+1
	s.store.complaints[complaint.ID] = complaint
	return nil
}

func (s *Service) AcceptComplaint(ctx context.Context, district, complaintID string, expectedVersion int) (Complaint, error) {
	operationCtx := acceptanceContextError(ctx)
	if err := operationCtx.Err(); err != nil {
		return Complaint{}, err
	}
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	complaint, ok := s.store.complaints[complaintID]
	if !ok {
		return Complaint{}, ErrNotFound
	}
	if complaint.DistrictID != district {
		return Complaint{}, ErrForbidden
	}
	if complaint.Version != expectedVersion {
		return Complaint{}, ErrConflict
	}
	if complaint.Status != "pending" || complaint.AssessmentID == "" || complaint.ZoneID == "" {
		return Complaint{}, ErrInvalidState
	}
	complaint.Status, complaint.Version = "accepted", complaint.Version+1
	s.store.complaints[complaint.ID] = complaint
	return cloneComplaint(complaint), nil
}

func (s *Service) VerifyPlan(ctx context.Context, district, planID string, expectedVersion int) (RectificationPlan, error) {
	if err := ctx.Err(); err != nil {
		return RectificationPlan{}, err
	}
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	plan, ok := s.store.plans[planID]
	if !ok {
		return RectificationPlan{}, ErrNotFound
	}
	if plan.DistrictID != district {
		return RectificationPlan{}, ErrForbidden
	}
	if plan.Version != expectedVersion {
		return RectificationPlan{}, ErrConflict
	}
	if plan.Status != "submitted" {
		return RectificationPlan{}, ErrInvalidState
	}
	plan.Status, plan.Version = "verified", plan.Version+1
	s.store.plans[plan.ID] = plan
	complaint := s.store.complaints[plan.ComplaintID]
	complaint.PlanID, complaint.Version = plan.ID, complaint.Version+1
	s.store.complaints[complaint.ID] = complaint
	return clonePlan(plan), nil
}

func (s *Service) CloseComplaint(ctx context.Context, district, complaintID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	complaint, ok := s.store.complaints[complaintID]
	if !ok {
		return ErrNotFound
	}
	if complaint.DistrictID != district {
		return ErrForbidden
	}
	plan, planOK := s.store.plans[complaint.PlanID]
	if complaint.Status != "accepted" || !planOK || plan.Status != "verified" {
		return ErrInvalidState
	}
	complaint.Status, complaint.Version = "closed", complaint.Version+1
	s.store.complaints[complaint.ID] = complaint
	return nil
}

func (s *Service) ReleaseAssessment(ctx context.Context, district, assessmentID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	assessment, ok := s.store.assessments[assessmentID]
	if !ok {
		return fmt.Errorf("release assessment %s: %w", assessmentID, ErrNotFound)
	}
	if assessment.DistrictID != district {
		return ErrForbidden
	}
	if assessment.Released {
		return ErrConflict
	}
	assessment.Released, assessment.Version = true, assessment.Version+1
	s.store.assessments[assessment.ID] = assessment
	return nil
}

func (s *Service) RetryPlanPublication(ctx context.Context, plan RectificationPlan, key string) (RectificationPlan, error) {
	result, err := s.PublishPlan(ctx, plan, key)
	if err == nil || !errors.Is(err, ErrStorage) {
		return result, err
	}
	return s.PublishPlan(ctx, plan, key)
}
