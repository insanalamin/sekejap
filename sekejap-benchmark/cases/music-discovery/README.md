# Music Discovery

## Workload Story

This suite represents music discovery and collection traversal:
- artists
- songs
- albums
- collections
- venues
- genre/style relations

Typical questions:
- from this artist, what songs and collections connect nearby?
- what tracks are similar to this one?

## Dataset Shape

Entities:
- `artist`
- `song`
- `album`
- `collection`
- `venue`
- `genre`

Victorian references:
- Fitzroy venues
- St Kilda live music
- Collingwood labels
- Geelong acts
- Melbourne indie scenes

Relations:
- `performed_by`
- `belongs_to_album`
- `in_collection`
- `similar_to`
- `played_at`

Fields:
- `created_at`
- `geometry`
- `title`
- `lyrics_or_notes`
- `embedding`

## Main Benchmark Cases

1. `artist_to_song_to_collection_traversal`
2. `similar_song_vector_search`
3. `venue_local_scene_graph_lookup`
4. `hybrid_graph_vector_text_discovery`
5. `collection_expansion_from_anchor_song`

## Fairness Notes

- vector is approximate in SQLite unless brute-force fallback is used
- graph is native in Sekejap and recursive in SQLite
- spatial is approximate in SQLite

## Primary Optimization Goal

Graph discovery with vector support should stay fast and composable.
