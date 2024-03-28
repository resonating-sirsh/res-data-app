### Shortcut Collector
Collects recent data from shortcut to better understand the work the team is doing

#### Configuration
You can set the BACKFILL flag in Argo to "true" to run for the full history. If you don't run for the full history, this will run for the prior 2 days each night, with deduplication in Snowflake.


### Payloads
Stories:
```
{
  "app_url": "https://app.shortcut.com/resonanceny/story/676",
  "archived": false,
  "blocked": false,
  "blocker": false,
  "branches": [],
  "comments": [
    {
      "app_url": "https://app.shortcut.com/resonanceny/story/676/make-cut-files-in-meta-one-for-fuse-and-stamper-pieces#activity-804",
      "author_id": "61f025ca-a6ce-439f-bd32-fb3d4b8b7e8c",
      "created_at": "2022-03-02T01:27:53Z",
      "deleted": false,
      "entity_type": "story-comment",
      "external_id": null,
      "group_mention_ids": [],
      "id": 804,
      "member_mention_ids": [
        "61d351c1-65e5-462a-b6b0-d659fd4e76f5"
      ],
      "mention_ids": [
        "61d351c1-65e5-462a-b6b0-d659fd4e76f5"
      ],
      "parent_id": null,
      "position": 1,
      "reactions": [],
      "story_id": 676,
      "text": "[@dewyn](shortcutapp://members/61d351c1-65e5-462a-b6b0-d659fd4e76f5) as i thought about what this queue is, although i was thinking of it living in cut, actually what cut cares about, i think, is that there are pieces in the print asset queue that require fuse and stamper pieces to be cut. So really what we should do is add a flag on the make one production queue or print asset table to say `has_fuse` and `has_stamper` - that way Cut an just create a view that tells them there is a need for these things to be cut somewhere down the line. I was just about to create some kafka topics and all sorts and then realized it doesnt make sense to do it that way. \nwhat i might do is link this all back to wherever we put the factory order pdf etc. so all the meta one resources are in one place or something",
      "updated_at": "2022-03-02T01:34:46Z"
    },
    {
      "app_url": "https://app.shortcut.com/resonanceny/story/676/make-cut-files-in-meta-one-for-fuse-and-stamper-pieces#activity-806",
      "author_id": "61d351c1-65e5-462a-b6b0-d659fd4e76f5",
      "created_at": "2022-03-02T13:04:50Z",
      "deleted": false,
      "entity_type": "story-comment",
      "external_id": null,
      "group_mention_ids": [],
      "id": 806,
      "member_mention_ids": [],
      "mention_ids": [],
      "parent_id": null,
      "position": 2,
      "reactions": [],
      "story_id": 676,
      "text": "Yeah I agree, cut shouldn't care for they don't need to action on. They still do need to mount the roll to the laser cutters and sort the pieces by ONE but that's another thing.",
      "updated_at": "2022-03-02T13:04:50Z"
    }
  ],
  "commits": [],
  "completed": false,
  "completed_at": null,
  "completed_at_override": null,
  "created_at": "2022-02-28T18:06:23Z",
  "custom_fields": [],
  "cycle_time": NaN,
  "deadline": null,
  "description": "these are generated from DXF by size and compensated for unstable materials and Ryon ??\nthese need to be uploaded [somewhere] in the cut app maybe by the same process that generated print assets for Male",
  "entity_type": "story",
  "epic_id": 251,
  "estimate": 8,
  "external_id": null,
  "external_links": [],
  "files": [],
  "follower_ids": [
    "61f025ca-a6ce-439f-bd32-fb3d4b8b7e8c",
    "61d351c1-65e5-462a-b6b0-d659fd4e76f5"
  ],
  "group_id": "61d3560c-6fd2-4767-9a3f-d4e3d401bc21",
  "group_mention_ids": [],
  "id": 676,
  "iteration_id": 1013,
  "label_ids": [],
  "labels": [],
  "lead_time": NaN,
  "linked_files": [],
  "member_mention_ids": [],
  "mention_ids": [],
  "moved_at": "2022-03-01T15:17:51Z",
  "name": "Make cut files in meta one for fuse and stamper pieces",
  "owner_ids": [
    "61f025ca-a6ce-439f-bd32-fb3d4b8b7e8c"
  ],
  "position": 32150727680,
  "previous_iteration_ids": [
    485
  ],
  "primary_key": "676_1647356037",
  "project_id": 411,
  "pull_requests": [],
  "requested_by_id": "61f025ca-a6ce-439f-bd32-fb3d4b8b7e8c",
  "run_time": 1647356037,
  "started": true,
  "started_at": "2022-03-01T15:17:51Z",
  "started_at_override": null,
  "stats": {
    "num_related_documents": 0
  },
  "story_links": [],
  "story_template_id": null,
  "story_type": "feature",
  "tasks": [
    {
      "complete": true,
      "completed_at": "2022-03-03T16:25:36Z",
      "created_at": "2022-03-01T22:01:40Z",
      "description": "ensure exists body/size for fuse&stamper and just fuse at body level store at `s3://meta-one-assets-prod/bodies/cut/[body_code]`",
      "entity_type": "story-task",
      "external_id": null,
      "group_mention_ids": [],
      "id": 799,
      "member_mention_ids": [],
      "mention_ids": [],
      "owner_ids": [],
      "position": 1,
      "story_id": 676,
      "updated_at": "2022-03-03T16:25:36Z"
    },
    {
      "complete": false,
      "completed_at": null,
      "created_at": "2022-03-01T22:02:41Z",
      "description": "create a test queue when we prep ordered pieces for Ivan to keep an eye on - maybe [here](https://airtable.com/appyIrUOJf8KiXD1D/tblwIFbHo4PsZbDgz/viwH4JMhjra0nft5G?blocks=hide)",
      "entity_type": "story-task",
      "external_id": null,
      "group_mention_ids": [],
      "id": 800,
      "member_mention_ids": [],
      "mention_ids": [],
      "owner_ids": [],
      "position": 2,
      "story_id": 676,
      "updated_at": "2022-03-03T20:17:42Z"
    },
    {
      "complete": false,
      "completed_at": null,
      "created_at": "2022-03-14T14:57:03Z",
      "description": "v-notches on cutlines as optional e.g. for knits but also generating the outline for wovens where required.",
      "entity_type": "story-task",
      "external_id": null,
      "group_mention_ids": [],
      "id": 1051,
      "member_mention_ids": [],
      "mention_ids": [],
      "owner_ids": [],
      "position": 3,
      "story_id": 676,
      "updated_at": "2022-03-14T14:57:03Z"
    },
    {
      "complete": false,
      "completed_at": null,
      "created_at": "2022-03-14T14:57:15Z",
      "description": "check in on the body export locations",
      "entity_type": "story-task",
      "external_id": null,
      "group_mention_ids": [],
      "id": 1052,
      "member_mention_ids": [],
      "mention_ids": [],
      "owner_ids": [],
      "position": 4,
      "story_id": 676,
      "updated_at": "2022-03-14T14:57:15Z"
    },
    {
      "complete": false,
      "completed_at": null,
      "created_at": "2022-03-14T17:13:21Z",
      "description": "add knit cut files with v-notches",
      "entity_type": "story-task",
      "external_id": null,
      "group_mention_ids": [],
      "id": 1079,
      "member_mention_ids": [],
      "mention_ids": [],
      "owner_ids": [],
      "position": 5,
      "story_id": 676,
      "updated_at": "2022-03-14T17:13:21Z"
    }
  ],
  "updated_at": "2022-03-14T17:15:28Z",
  "workflow_id": 500000019,
  "workflow_state_id": 500000022
}
```
Milestones:
```
{
  "app_url": "https://app.shortcut.com/resonanceny/milestone/1091",
  "categories": [],
  "completed": false,
  "completed_at": null,
  "completed_at_override": null,
  "created_at": "2022-03-15T14:10:31Z",
  "description": "https://coda.io/d/Technology_dZNX5Sf3x2R/reFrame-Healing-Flow_su9HG#_luTt3",
  "entity_type": "milestone",
  "global_id": "v2:m:61cf4b68-aadb-4b84-9d01-06c24d33ccd9:1091",
  "id": 1091,
  "name": "reFrame: Healing Flow",
  "position": 28160,
  "primary_key": "1091_1647356037",
  "run_time": 1647356037,
  "started": false,
  "started_at": null,
  "started_at_override": null,
  "state": "to do",
  "stats": {
    "num_related_documents": 0
  },
  "updated_at": "2022-03-15T14:10:31Z"
}
```

Epics:
```
{
  "app_url": "https://app.shortcut.com/resonanceny/epic/615",
  "archived": false,
  "completed": false,
  "completed_at": null,
  "completed_at_override": null,
  "created_at": "2022-02-28T16:58:39Z",
  "deadline": "2022-03-04T04:00:00Z",
  "entity_type": "epic",
  "epic_state_id": 500000002,
  "external_id": null,
  "follower_ids": [
    "6213b55a-d25c-471e-96df-320a1345ef4c"
  ],
  "global_id": "v2:e:61cf4b68-aadb-4b84-9d01-06c24d33ccd9:615",
  "group_id": "620bd0b9-2a9c-4257-be2f-36d2a4767318",
  "group_mention_ids": [],
  "id": 615,
  "label_ids": [],
  "labels": [],
  "member_mention_ids": [],
  "mention_ids": [],
  "milestone_id": 560,
  "name": "MAKE Bots -> res.Magic.Print Bots",
  "owner_ids": [
    "6213b55a-d25c-471e-96df-320a1345ef4c"
  ],
  "planned_start_date": "2022-02-28T04:00:00Z",
  "position": 38,
  "primary_key": "615_1647016668",
  "productboard_id": null,
  "productboard_name": null,
  "productboard_plugin_id": null,
  "productboard_url": null,
  "project_ids": [],
  "requested_by_id": "6213b55a-d25c-471e-96df-320a1345ef4c",
  "run_time": 1647016668,
  "started": false,
  "started_at": null,
  "started_at_override": null,
  "state": "to do",
  "stats": {
    "average_cycle_time": 603206,
    "average_lead_time": 603206,
    "last_story_update": "2022-03-08T13:42:57Z",
    "num_points": 0,
    "num_points_done": 0,
    "num_points_started": 0,
    "num_points_unstarted": 0,
    "num_related_documents": 0,
    "num_stories_done": 1,
    "num_stories_started": 0,
    "num_stories_total": 1,
    "num_stories_unestimated": 1,
    "num_stories_unstarted": 0
  },
  "stories_without_projects": 1,
  "updated_at": "2022-02-28T16:58:39Z"
}
```
