/*
 * Copyright 2018 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Dgraph Community License (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 *     https://github.com/dgraph-io/dgraph/blob/master/licenses/DCL.txt
 */

package worker

import (
	"github.com/dgraph-io/dgraph/ee/backup"
	"github.com/dgraph-io/dgraph/posting"
	"github.com/dgraph-io/dgraph/protos/pb"
	"github.com/dgraph-io/dgraph/x"

	"github.com/golang/glog"
	"golang.org/x/net/context"
)

func restoreProcess(ctx context.Context, req *pb.BackupRequest) error {
	glog.Infof("Restore request: group %d at %d", req.GroupId, req.ReadTs)
	if err := ctx.Err(); err != nil {
		glog.Errorf("Context error during backup: %v\n", err)
		return err
	}
	// sanity, make sure this is our group.
	if groups().groupId() != req.GroupId {
		return x.Errorf("Restore request group mismatch. Mine: %d. Requested: %d\n",
			groups().groupId(), req.GroupId)
	}
	// wait for this node to catch-up.
	if err := posting.Oracle().WaitForTs(ctx, req.ReadTs); err != nil {
		return err
	}
	// create backup request and process it.
	br := &backup.Request{DB: pstore, Backup: req}
	return br.Restore(ctx)
}

// Restore handles a request coming from another node.
func (w *grpcWorker) Restore(ctx context.Context, req *pb.BackupRequest) (*pb.Status, error) {
	var resp pb.Status
	glog.V(2).Infof("Received restore request via Grpc: %+v", req)
	if err := restoreProcess(ctx, req); err != nil {
		resp.Code = -1
		resp.Msg = err.Error()
		return &resp, err
	}
	return &resp, nil
}

func restoreGroup(ctx context.Context, in pb.BackupRequest) error {
	glog.V(2).Infof("Sending restore request: %+v\n", in)
	// this node is part of the group, process restore.
	if groups().groupId() == in.GroupId {
		return restoreProcess(ctx, &in)
	}

	// send request to any node in the group.
	pl := groups().AnyServer(in.GroupId)
	if pl == nil {
		return x.Errorf("Couldn't find a server in group %d", in.GroupId)
	}
	status, err := pb.NewWorkerClient(pl.Get()).Restore(ctx, &in)
	if err != nil {
		glog.Errorf("Restore error group %d: %s", in.GroupId, err)
		return err
	}
	if status.Code != 0 {
		err := x.Errorf("Restore error group %d: %s", in.GroupId, status.Msg)
		glog.Errorln(err)
		return err
	}
	glog.V(2).Infof("Restore request to gid=%d. OK\n", in.GroupId)
	return nil
}

// RestoreOverNetwork handles a request coming from an HTTP client.
func RestoreOverNetwork(pctx context.Context, source string) error {
	ctx, cancel := context.WithCancel(pctx)
	defer cancel()

	// Check that this node can accept requests.
	if err := x.HealthCheck(); err != nil {
		glog.Errorf("Restore canceled, not ready to accept requests: %s", err)
		return err
	}

	// Get ReadTs from zero.
	ts, err := Timestamps(ctx, &pb.Num{Val: 1})
	if err != nil {
		glog.Errorf("Unable to retrieve timestamp for restore: %s", err)
		return err
	}

	gids := groups().KnownGroups()
	req := pb.BackupRequest{
		ReadTs: ts.EndId,
		Source: source,
	}
	glog.Infof("Created restore request: %+v. Groups=%v\n", req, gids)

	// This will dispatch the request to all groups and wait for their response.
	// If we receive any failures, we cancel the process.
	errCh := make(chan error, 1)
	for _, gid := range gids {
		req.GroupId = gid
		go func() {
			errCh <- restoreGroup(ctx, req)
		}()
	}

	for i := 0; i < len(gids); i++ {
		err := <-errCh
		if err != nil {
			glog.Errorf("Error received during restore: %v", err)
			return err
		}
	}
	req.GroupId = 0
	glog.Infof("Restore for req: %+v. OK.\n", req)
	return nil
}