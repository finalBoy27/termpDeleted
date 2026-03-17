# G2 Web (Admin + Client) with Postgres (Railway) + Mongo support

This converts `g2.py` into a small web app:

- **Admin** logs in and scrapes a username (2019–2026) **one time** and stores results in **Postgres (Railway)** by default.
- **Client** logs in and searches a username; the **media gallery UI is the same design from `g2.py`**, but data comes directly from DB (no re-scrape).

## Accounts (defaults)

- **Client**: `client` / `dev007`
- **Admin**: `admin` / `devpsw`

Change them via env vars (recommended for deploy).

## Project structure

- `g2.py`: your original scraper + gallery generator (kept)
- `webapp/`: Flask web app
  - `app.py`: routes (admin/client/gallery)
  - `scraper.py`: scrape → DB (Postgres/Mongo)
  - `storage.py`: DB backend switch (Postgres first)
  - `pg.py`: Postgres schema (Railway)
  - `db.py`: Mongo connection + indexes (optional)
  - `auth.py`: session auth (admin/client)
- `requirements.txt`: Python deps
- `.env.example`: env template
- `Procfile`: for Render / some Railway setups

## Local run (Windows)

### 1) Create venv + install

```bash
python -m venv .venv
.venv\\Scripts\\activate
pip install -r requirements.txt
```

### 2) Choose database backend

#### Option A (recommended): Railway Postgres

Set in `.env`:

```env
DB_BACKEND=postgres
DATABASE_URL=postgresql+asyncpg://...   # Railway URL also works; app converts it for sync driver
```

#### Option B: MongoDB

Set in `.env`:

```env
DB_BACKEND=mongo
MONGODB_URI=mongodb://localhost:27017
MONGODB_DB=g2media
```

- **Local MongoDB** (installed locally): use default `mongodb://localhost:27017`
- **MongoDB Atlas Free**: create a free cluster and copy the connection string

### 3) Create `.env`

Copy `.env.example` to `.env` and set your DB config.

Example (Railway Postgres):

```env
DB_BACKEND=postgres
DATABASE_URL=postgresql+asyncpg://postgres:PASS@HOST:PORT/railway
SECRET_KEY=some-random-long-string
```

Example (Mongo Atlas):

```env
DB_BACKEND=mongo
MONGODB_URI=mongodb+srv://USER:PASS@cluster0.xxxxx.mongodb.net/?retryWrites=true&w=majority
MONGODB_DB=g2media
SECRET_KEY=some-random-long-string
```

### 4) Run the web server

```bash
uvicorn webapp_fastapi.main:app --host 0.0.0.0 --port 8000 --reload
```

Open:

- Home: `http://localhost:8000/`
- Admin: `http://localhost:8000/admin/login`
- Client: `http://localhost:8000/client/login`

## How to use

1) Login as **admin** and scrape a username (this stores in DB).
2) Login as **client** and search the same username → gallery opens instantly from DB.

### Multiple usernames (client)

You can search multiple usernames separated by commas:

Example:

- `tanu jain, some other, third name`

### Date range (admin)

Admin supports:

- **Year only**: `2019` to `2026`
- **Full date**: `2019-01-01` to `2026-12-31`

### Admin CRUD

- **Cancel running job**: use the Cancel button in Recent jobs (cooperative cancel).
- **Delete cached username**: deletes that username’s media + cache metadata (case-insensitive).

## Deploy (easy)

### Option A: Render (simple)

1) Push this folder to GitHub.
2) On Render → **New Web Service** → connect repo.
3) Set:
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `uvicorn webapp_fastapi.main:app --host 0.0.0.0 --port $PORT`
4) Add **Environment Variables**:
   - `MONGODB_URI` (Atlas recommended)
   - `MONGODB_DB` (example `g2media`)
   - `SECRET_KEY` (random)
   - `ADMIN_USER`, `ADMIN_PASSWORD`, `CLIENT_USER`, `CLIENT_PASSWORD`

### Option B: Railway

1) Push to GitHub.
2) Create a new Railway project from the repo.
3) Add env vars:
   - `DB_BACKEND=postgres`
   - `DATABASE_URL=...` (Railway)
   - plus `SECRET_KEY`, and account env vars if you want to change defaults
4) Deploy.

## Notes / limits

- **Caching rule**: if a username is already cached for the year range, admin won’t re-scrape unless **Force** is checked.
- This version stores **URLs only** (images/videos/gifs), not downloading media files.


i want in this quened scaping dont sape all username running conccuer querd 1 comepte then enxt process that not load on site also admin panel mutpler usename input supprberl separecte with commern to proecss sepratly
alos in client side supposer susernme haivn


"xyz abc" 

supposer use type only xyz of abc then display that only so dont show not resul tfout for that