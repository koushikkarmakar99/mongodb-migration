#!/bin/bash

echo "--- Stopping and Removing Containers, Networks, and Volumes ---"
podman compose down -v

echo "--- Cleanup Complete ---"
