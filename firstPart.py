import pandas as pd
import json

# Class to read an API call from Spotify which contains an artists and its albums and songs


class Spotify_reader:
    spotify_db: dict
    artist_db: pd
    albums_db: pd
    tracks_db: pd
    audio_features: pd
    export_db: pd

    # Read the song from a json file that is in the data folder
    def __init__(self, file: str):
        with open(f"./data/{file}") as file:
            self.spotify_db = json.load(file)

    # Create a dataframe with the data of the artist. This could be latter improved if in one call comes several artists
    def get_artists(self):
        only_artist = dict(self.spotify_db)
        del only_artist["albums"]
        self.artist_db = pd.DataFrame.from_dict([only_artist])

    # Create a dataframe with the data of the albums
    def get_albums(self):
        parent_artist = {"album.artist_id": self.spotify_db.get("artist_id", None)}
        albums_data = self.spotify_db.get("albums", [])
        only_albums = [
            {key: value for key, value in album.items() if key != "tracks"}
            | parent_artist
            for album in albums_data
        ]
        self.albums_db = pd.DataFrame(only_albums)

    # Create a dataframe with the data from each track, the response included duplicated albums_ids
    def get_tracks(self):
        only_tracks = []
        audio_features = []
        for album in self.spotify_db.get("albums", []):
            parent_album = {"track.album_id": album.get("album_id", None)}
            for track in album.get("tracks", None):
                song_data = {
                    key: value
                    for key, value in track.items()
                    if key != "audio_features"
                }
                audio_data = {
                    f"audio_features.{key}": value
                    for key, value in track.get("audio_features", "{}").items()
                }
                song_data.update(parent_album)
                only_tracks.append(song_data)
                audio_features.append(audio_data)
        self.tracks_db = pd.DataFrame(only_tracks)
        self.audio_features = pd.DataFrame(audio_features)

    def data_breakdown(self):
        self.get_artists()
        self.get_albums()
        self.get_tracks()

    # This approach returned bad results because there are songs that are missing a track_id
    def export_csv_steps(self):
        self.data_breakdown()
        audio_features = self.audio_features.drop_duplicates()
        tracks_db = self.tracks_db.drop_duplicates()
        artist_db = self.artist_db.drop_duplicates()
        albums_db = self.albums_db.drop_duplicates()

        track_features = pd.merge(
            tracks_db,
            audio_features,
            how="left",
            left_on="track_id",
            right_on="audio_features.id",
        )
        artist_album = pd.merge(
            artist_db,
            albums_db,
            how="right",
            left_on="artist_id",
            right_on="album.artist_id",
        )
        export_csv = pd.merge(
            track_features,
            artist_album,
            how="left",
            left_on="track.album_id",
            right_on="album_id",
        ).drop(columns=["album.artist_id", "track.album_id"])
        export_csv.to_csv("dataset_steps.csv", index=False)

    # Export a csv with a single read of the json file
    def export_csv(self):
        exploded_dict = []
        # Dictionary with the artist info
        artist_data = dict(self.spotify_db)
        del artist_data["albums"]

        for album in self.spotify_db.get("albums", []):
            album_data = {key: value for key, value in album.items() if key != "tracks"}
            for track in album.get("tracks", []):
                song_data = {
                    key: value
                    for key, value in track.items()
                    if key != "audio_features"
                }
                audio_data = {
                    f"audio_features.{key}": value
                    for key, value in track.get("audio_features", "{}").items()
                }
                exploded_line = dict(song_data | audio_data | artist_data | album_data)
                exploded_dict.append(exploded_line)
        self.export_db = pd.DataFrame(exploded_dict)
        self.export_db.to_csv("dataset.csv", index=False)


if __name__ == "__main__":
    filename = "taylor_swift_spotify.json"
    handler = Spotify_reader(filename)
    handler.export_csv_steps()
    handler.export_csv()
