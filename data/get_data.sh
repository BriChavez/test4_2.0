#!/bin/bash
# gsutil -m cp gs://data.datastack.academy/spotify/new_tracks.json .
gsutil -m cp gs://data.datastack.academy/spotify/spotify_artists.csv gs://data.datastack.academy/spotify/spotify_albums.csv .
# This will copy the Spotify CSV files to your data/ directory.