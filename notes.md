# Cluebase 2.0 devlog

It's been about 5 years since I created [Cluebase](cluebase.luke.lav.in), a general-use API for retrieving Jeopardy! data of all sorts--clues, episodes, contestants, etc. Since then, I've received more questions, requests, and even success stories ([Congrats, Kate!](https://www.youtube.com/watch?v=AKN-SKdbYWo)) than I could have ever expected. Unfortunately, since its inception Cluebase has been pretty flawed.

Cluebase's primary purpose was not to be a quality API, or to be useful for some other application, but rather just for me to gain some practical software engineering experience during a college summer where I had no internship. I had some vague idea of using Cluebase after its creation to build a Jeopardy! Trainer app, but for one reason or another, the project fell by the wayside.

## Phase 1: Scraping

Let's start by looking at the existing process and how it can be improved

Previous problems:

#### 1. Data Completeness  

Clues with videos/images/audio that become unanswerable with text only. Perfect world we could use all these clues, but want to at least be able to identify and exclude them in text-only use cases.

Most clues with supplementary material have the material linked, which could both provide a way to use the supplementary material in the future and also a way to identify which clues are unusable right now.

Some clues require supplementary material but do not have the supplementary material archived. These clues are not only unusable now and in the future, but they are hard to find as they are not denoted in a standardized way, usually with some [editorial notes] or ellipsis..., undifferentiated from other irrelevant commentary that takes the same form.

#### 2. Coupled Scrape/Load process

Previously the scraping was coupled with parsing and loading into the db. This made changing any parsing/loading/formatting processes require downloading all existing html pages all over again, which takes hours while under any rate limit respectful to the fragile j-archive servers. 

I would like to ideally have a "data lake" of raw HTML, which can be processed and built upon by separate processes. Doing so will also greatly aid in solving the challenges in the actual load process, which will be the next blog entry.


### Plan:

Basic web crawler/scraper BFS approach:

Start at the root of the site. Download the root page, and grab all links from the page. Then, for each link, download the page, and repeat.



### Dev progress:

Currently: Focus on keeping good raw HTML data.

- Downloading season list HTML
- Scraping season list HTML for season links, then downloading those HTML files
- Scraping season links for game links, then downloading those HTML files

Current problems I'm running into are related to the process being long and flaky. While the current flakiness comes from me starting and stopping the process frequently due to active development, network and file actions are inherently flaky, so a long running process dependent on the stability of these is asking for problems. 

The constant start and stop of the process raises the following concerns:

1. Data integrity: How do we know a file was fully written, or properly written. How do we know a request completed successfully, and returned the full HTML we want.
2. Processing status: How do we know when we've already processed an HTML link. Can we skip files we already have to try to just pick back where we left off? If we do, how do we know when a file is out of date, and actually needs to be updated?

TODO: add some validation checks on html files. Most of the 

Only intelligence is that it will not overwrite a html file that already exists
 - is this how it should work? what if a page exists but is only partially complete (this happens every night!)
 - maybe won't matter if there's a scheduled "refresh" that ignores overwrite protection


## Phase 2: Parsing Basic Clue Info

First thing I want to do is just grab all clues from the games and put them in a generally usable format. Easy to iterate on this process because whereas scrape/download time is close to 7 *hours*, parsing is closer to 7 *minutes*. As such, focus should be on simple extraction and getting a minimum viable data product, so that improvements/alterations can be made along the way to fit future data needs.

Ultimately, the challenges to address here are the following:

1. Storage
2. Data Quality
3. Idempotence

#### 1. Storage

Once the clue info is parsed, what's the best way to store it? 

It's tough to answer this question without knowing the exact use cases for the data, as the best data storage is the one that works for the application. For this reason, I want both the format and the framework of storage to be generic and uncomplicated.

- Flat file
 
This would be the simplest in terms of access. Flat files with no real structure, just plain data. Because the clues have a lot more useful info than just the clue itself (correct response, category, difficulty, date, etc), this would likely be a wide CSV. 

Any use of a file like this would need a package or a decent amount of logic to parse this CSV to where each column is accessible easily. Doing any manual analysis with this file would take a non-zero amount of coding.

Simple to write, slightly annoying to query against

- Big JSON file full of clue objects

One big *structured* file includes extra clue info like category, difficulty, air date, the game it comes from, all directly on the objects themselves. Easier to query than the flat file, but still likely requires some code to do filtering, looping, aggegrating.

Less simple to write, slightly less annoying to query against

- SQL.

The most complicated to write, requires a strict schema. 

Exporting to a generic usable form is easy, though, and querying is familiar and simple.

- Mongo. This essentially is the same as having a big JSON file, but makes it a little easier to query both programmatically and for ad hoc analysis in 3T. Can easily go from this to a big JSON file in one step.

Best of both worlds. Easy query interface, easy to export to other formats, simple to load.

#### 2. Data Quality
- Seasons have links to pages other than games (YT videos, etc)
- Tape date vs air date (trebek pilots)
- Games w/ missing rounds entirely (6737)
- Some games treat fill missing clues with placeholders (6737 fills them with "=")
- Identify duplicate clues on load
- Editorial comments on categories

> Seasons have links to pages other than games (YT videos, etc)
- Ensure the only links parsed are "relative" links, without any domain included
- Simplest for this purpose was just to ensure link doesn't start with http

#### Tape date vs air date

- Can't search game page for "aired" or anything like that, need to search for date in particular
- Regex to grab date in the page title

#### Missing rounds

#### "Empty" clues

#### Editorial comments
Alex ()

#### Idempotence

- Clues can be identified by the game they are in, as well as their place on the game board.
- Construct a clue id made up of the game id, round number, category index, and row number
- Let mongo catch duplicates by making this compound id the `_id` field.




### Prefect steps

Install prefect

Authenticate prefect cloud CLI

Create flows/tasks

Run flow to test

