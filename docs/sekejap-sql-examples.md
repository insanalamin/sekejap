# Sekejap SQL Examples

These are short workload-oriented examples for the planned benchmark suites.


## Collection And Insert Examples

### Indexed Collection Definition

```sql
CREATE COLLECTION memories (
  id UUID PRIMARY KEY DEFAULT uuidv4(),
  title TEXT,
  story TEXT,
  created_at TIMESTAMP,
  remembered_time VAGUE_TIME,
  geometry GEOMETRY,
  embedding VECTOR(768),
  weather TEXT
) WITH (
  hash_index = [id, weather],
  range_index = [created_at],
  temporal_index = [remembered_time],
  spatial_index = [geometry],
  vector_index = [embedding],
  fulltext_index = [title, story]
);
```

### Plain Node Insert

```sql
INSERT INTO causes (id, title, body, created_at)
VALUES (
  'wet_road_00001',
  'Wet road',
  'Standing water reduced traction during the crash.',
  TIMESTAMP '2024-06-15 09:30:00'
);
```

### Vector Insert

```sql
INSERT INTO researchers (id, name, embedding)
VALUES (
  'researcher_00001',
  'Alya Pranoto',
  [0.12, 0.08, 0.44, 0.91]
);
```

### Vague Time Insert

```sql
INSERT INTO memories (id, title, remembered_time)
VALUES (
  'memory_00001',
  'Afternoons in the lab',
  '{"bounds":{"startYear":2019,"endYear":2020},"constraints":{"months":[3,4],"weekdays":[1,2,3,4,5],"timeOfDay":{"startMinute":780,"endMinute":1020,"fuzzyRadiusMinute":20}},"globalFuzziness":0.15}'
);
```

### Spatial Insert

```sql
INSERT INTO places (id, title, geometry)
VALUES (
  'place_woodside_lab',
  'Woodside Lab',
  '{"type":"Point","coordinates":[145.1360,-37.9100]}'
);
```

### Hybrid Multimodal Insert

```sql
INSERT INTO memories (
  id,
  title,
  story,
  created_at,
  remembered_time,
  geometry,
  embedding,
  weather
)
VALUES (
  'memory_00002',
  'Desk by the window',
  'Quiet afternoon in the lab, cloudy light, unfinished notes.',
  TIMESTAMP '2024-01-12 14:43:01',
  '{"bounds":{"startYear":2024,"endYear":2024},"constraints":{"months":[1],"daysOfMonth":[12],"timeOfDay":{"startMinute":883,"endMinute":883,"fuzzyRadiusMinute":0}},"globalFuzziness":0.0}',
  '{"type":"Point","coordinates":[145.1360,-37.9100]}',
  [0.11, 0.24, 0.53, 0.72],
  'cloudy'
);
```

## 1. Memory-Time-Space

Find memories near a place during a vague remembered period.

```sql
SELECT m.title
FROM memories m
TRAVERSE FORWARD located_in TO places p
WHERE VAGUE_TIME_INTERSECTS(m.time, START_YEAR 2019, END_YEAR 2020)
  AND ST_DWithin(p.geometry, POINT(145.1360, -37.9100), 1.5)
LIMIT 20;
```

## 2. Causal-Investigation

Trace likely causes from one incident.

```sql
SELECT c.title
FROM incidents i
TRAVERSE FORWARD caused_by TO causes c HOPS 10
WHERE i.id = 'incident_00001'
LIMIT 50;
```

Trace causes constrained by textual evidence.

```sql
SELECT c.title
FROM incidents i
TRAVERSE FORWARD caused_by TO causes c HOPS 10
WHERE i.id = 'incident_00001'
  AND c.body ILIKE '%poor education%'
LIMIT 50;
```

## 3. Life-Knowledge-Graph

Find health effects connected to a food habit.

```sql
SELECT h.title
FROM foods f
TRAVERSE FORWARD affects TO health_effects h HOPS 3
WHERE f.title = 'batagor'
LIMIT 20;
```

## 4. Research-Network

Find researchers near Melbourne working on similar topics.

```sql
SELECT r.name
FROM researchers r
WHERE VECTOR_NEAR(r.embedding, $1, 50)
  AND ST_DWithin(r.geometry, POINT(144.9631, -37.8136), 25.0)
LIMIT 20;
```

## 5. Music-Discovery

Expand from an artist to songs.

```sql
SELECT s.title
FROM artists a
TRAVERSE FORWARD performed_by TO songs s
WHERE a.name = 'Courtney Barnett'
LIMIT 30;
```

## 6. Learning-Paths

Find prerequisite concepts for a target concept.

```sql
SELECT p.title
FROM concepts c
TRAVERSE BACKWARD prerequisite_for TO concepts p HOPS 5
WHERE c.title = 'deep learning'
LIMIT 50;
```

## Weighted Hybrid Retrieval

Weighted retrieval is critical when graph, time, space, vector, and text all matter together.

### Causal Ranking

```sql
SELECT c.title, score()
FROM incidents i
TRAVERSE FORWARD caused_by TO causes c HOPS 10
WHERE i.id = 'incident_00001'
  AND MATCHING(c.body, 'road wet drainage maintenance') WEIGHT 0.35
  AND VECTOR_NEAR(c.embedding, $1, 50) WEIGHT 0.20
  AND ST_DWithin(c.geometry, POINT(144.9631, -37.8136), 50.0) WEIGHT 0.10
  AND created_at >= TIMESTAMP '2024-01-01 00:00:00' WEIGHT 0.10
  AND VAGUE_TIME_NEAR(c.time, YEAR 2024, MONTHS [6,7]) WEIGHT 0.25
ORDER BY score() DESC
LIMIT 20;
```

### Memory Retrieval

```sql
SELECT m.title, score()
FROM memories m
TRAVERSE FORWARD located_in TO places p
WHERE MATCHING(m.story, 'woodside lab desk') WEIGHT 0.30
  AND VECTOR_NEAR(m.embedding, $1, 50) WEIGHT 0.30
  AND ST_DWithin(p.geometry, POINT(145.1360, -37.9100), 1.5) WEIGHT 0.15
  AND VAGUE_TIME_NEAR(m.time, YEAR 2019, MONTHS [3,4]) WEIGHT 0.25
ORDER BY score() DESC
LIMIT 20;
```

## Graph Writes

Single relation:

```sql
RELATE incidents/incident_00001 -> caused_by -> causes/wet_road_00001;
```

Relation with metadata:

```sql
RELATE incidents/incident_00001 -> caused_by -> causes/wet_road_00001
WEIGHT 0.92
META {"source":"regional_report","confidence":0.82};
```

Batch relations:

```sql
RELATE MANY (
  incidents/incident_00001 -> caused_by -> causes/wet_road_00001,
  causes/wet_road_00001 -> caused_by -> causes/drainage_00001,
  causes/drainage_00001 -> caused_by -> causes/maintenance_00001 WEIGHT 0.7
);
```



## Aggregation Examples

### Grade Rollup By Course

```sql
SELECT course.title, AVG(answer.grade)
FROM programmes p
TRAVERSE FORWARD has_course TO courses course
TRAVERSE FORWARD has_classroom TO classrooms classroom
TRAVERSE FORWARD has_student TO students student
TRAVERSE FORWARD submitted TO assessment_answers answer
WHERE p.id = 'programme_001'
GROUP BY course.title;
```

### Student Count By Classroom

```sql
SELECT classroom.name, COUNT(student.id)
FROM courses c
TRAVERSE FORWARD has_classroom TO classrooms classroom
TRAVERSE FORWARD has_student TO students student
WHERE c.id = 'course_001'
GROUP BY classroom.name;
```

### Planned Median Example

```sql
SELECT course.title, MEDIAN(answer.grade)
FROM programmes p
TRAVERSE FORWARD has_course TO courses course
TRAVERSE FORWARD has_classroom TO classrooms classroom
TRAVERSE FORWARD has_student TO students student
TRAVERSE FORWARD submitted TO assessment_answers answer
WHERE p.id = 'programme_001'
GROUP BY course.title;
```
