# Python base (zotify is a Python CLI) + Node 20 for the express wrapper.
# Debian slim avoids the musl/wheel pain that alpine hits on Pillow/pycryptodome.
FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive PIP_NO_CACHE_DIR=1

RUN apt-get update && apt-get install -y --no-install-recommends \
      ffmpeg git curl ca-certificates build-essential \
      libchromaprint-tools \
 && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
 && apt-get install -y --no-install-recommends nodejs \
 && rm -rf /var/lib/apt/lists/*

# The maintained fork (original zotify-dev repo is stale post Aug-2025 login change).
RUN pip install "git+https://github.com/Googolplexed0/zotify.git"

# beets = MusicBrainz auto-tagger + directory organizer (the "retag/rename" step).
# pyacoustid: chroma fingerprint matching; bs4/langdetect: lyrics plugin.
RUN pip install beets pyacoustid requests beautifulsoup4 langdetect

WORKDIR /app

COPY package*.json ./
RUN npm install --production

COPY src ./src
COPY public ./public

EXPOSE 3106

CMD ["npm", "run", "start"]
