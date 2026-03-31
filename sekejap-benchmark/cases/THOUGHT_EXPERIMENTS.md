# Thought Experiments

This file captures realistic Sekejap workload combinations.

The goal is not only to benchmark isolated atoms.
The goal is to think through how:

- graph
- exact time
- vague time
- spatial
- vector
- text
- weight

should combine in real products.

## 1. Civic News Graph

Shape:

- summarized news item
- source URL
- topic/category
- actors
- locations
- cause/effect links between events

Important domains:

- graph
- text
- exact time
- weight

Examples:

- start from one incident summary, traverse to upstream causes, then inspect supporting source links
- rank connected summaries by recency, source strength, and textual relevance

Likely planner pattern:

1. graph anchor if one summary is selected
2. graph traversal to related summaries / causes
3. exact time refine
4. text refine
5. weight by source strength + recency + graph proximity

Closest current case:

- `causal-investigation`

## 2. Educational Analytics

Shape:

- programmes
- courses
- classrooms
- students
- assessment answers
- grades

Important domains:

- graph
- tabular aggregation
- exact time

Examples:

- average grade by course inside one programme
- median score by classroom
- count of failing students by prerequisite path

Likely planner pattern:

1. graph anchor on programme or course
2. traverse to assessments / answers
3. exact time refine if semester/date filter exists
4. aggregation (`GROUP BY`, `AVG`, `MEDIAN`, `COUNT`)

Closest current case:

- `learning-paths`

## 3. Learning Path Discovery

Shape:

- concepts
- prerequisites
- courses
- learning goals

Important domains:

- graph
- text
- weight

Examples:

- what subjects help someone become a data engineer
- what concepts must be mastered before deep learning

Likely planner pattern:

1. graph anchor on target concept / role
2. backward prerequisite traversal
3. optional text refine on course metadata
4. weight by graph distance, importance, and learning path relevance

Closest current case:

- `learning-paths`

## 4. Research Cluster Space

Shape:

- researchers
- topics
- institutions
- campuses
- topic vectors
- researcher-topic graph

Important domains:

- vector
- graph
- spatial
- weight

Examples:

- cluster researchers in 3D by topic vectors
- place researcher node near strongest topic centroid while still showing topic graph edges
- find nearby topic clusters around electric vehicles, education, health AI

Likely planner pattern:

1. vector seed on topic similarity
2. graph refine through researcher-topic links
3. optional spatial refine on campus / region
4. weight by topic strength, graph closeness, and geographic proximity

Closest current case:

- `research-network`

## 5. Music Discovery

Shape:

- artists
- songs
- collections
- scenes
- venues
- lyric vectors
- artist/song relation graph

Important domains:

- vector
- graph
- text
- spatial
- weight

Examples:

- related songs by lyric vector plus graph of scene relations
- punk and emocore from Bandung/Soreang should outrank unrelated pop from elsewhere

Likely planner pattern:

1. vector seed on lyrics / semantic similarity
2. graph refine via artist-song-collection-scene relations
3. text refine on genre or tags
4. spatial refine on city/scene proximity
5. weight by vector similarity + graph closeness + locality

Closest current case:

- `music-discovery`

## 6. Life Problem / Article Retrieval

Shape:

- articles
- categories
- supporting text passages
- related concepts
- semantic embeddings

Important domains:

- graph
- vector
- text
- weight

Examples:

- related articles based on category + semantic similarity
- surface both direct category links and semantically adjacent articles

Likely planner pattern:

1. graph anchor or category anchor if present
2. vector or text seed if no graph anchor
3. graph refine on related concepts / article links
4. weight by category relevance + semantic similarity + passage match

Closest current case:

- `life-knowledge-graph`

## 7. Wiki Terminology Web

Shape:

- articles
- terms
- glossary links
- related concepts

Important domains:

- graph
- text
- weight

Examples:

- article to terminology graph
- expand from one article into connected terms and related articles

Likely planner pattern:

1. graph anchor on article or term
2. text refine on terminology
3. weight by graph distance and lexical relevance

Closest current case:

- `life-knowledge-graph`
- `learning-paths`

## 8. Memory Retrieval Around Time and Space

Shape:

- memories
- places
- related memories
- exact creation time
- vague remembered time

Important domains:

- exact time
- vague time
- spatial
- graph
- text
- weight

Examples:

- memories around a place and remembered period
- related memories nearby in time and space

Likely planner pattern:

1. exact time or spatial seed when sharp enough
2. vague time refine
3. graph expansion if related-memory traversal is requested
4. text refine
5. weight by place closeness + temporal closeness + graph relevance

Closest current case:

- `memory-time-space`

## Planner Implication

These workloads show why Sekejap cannot use one global fixed order like:

- always graph first
- always time last

Better rules:

1. anchored graph first
2. exact time before vague time
3. vague time usually later than sharper seeds
4. vector/text/spatial often act as seeds in unanchored retrieval
5. weight is final fusion, not seed
