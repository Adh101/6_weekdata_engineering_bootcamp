# KPIs and Experimentation Assignment

## Product Name: Mindtrip.ai

### Product Description
Mindtrip is an AI-driven travel planning platform that helps you discover, plan, and organize trips through conversational AI. It offers personalized suggestions, customizable itineraries, vibrant photos, interactive maps, reviews, and seamless collaboration features.

---

## My Journey with Product

### 1. Discovery & Sign-up
- Heard about Mindtrip from a travel blog / social media video showing AI creating itineraries from a photo.
- Signed up on the web app — quick onboarding asked for travel style, favorite activities, and preferred pace.
- First delight: “Plan a trip” — randomly selected a place, got a visually rich, clickable itinerary in under a minute.

### 2. Exploration Phase (First week)
- Played with conversational AI to refine the trip:
  - “Make this 5 days instead of 7”
  - “Add more coffee shops and art galleries”
- Loved the dynamic map — clicked pins, read integrated reviews, saved a few to trip plan.
- Shared itinerary with partner via link; both could edit in real time.

### 3. Habit Formation (Month 1–2)
- Used Mindtrip for multiple destinations: upcoming vacation + weekend getaways.
- Discovered “Nearby” feature in mobile app during a trip — instantly surfaced hidden gems within walking distance.
- Trusted the recommendations enough to use them on the fly without cross-checking on Google.

### 4. Power User (Ongoing)
- Uses it as primary trip planner + discovery engine.
- Keeps “wishlist” itineraries for dream trips.
- Uses “magic camera” to snap places while traveling and add to plan instantly.
- Shares itineraries socially to inspire friends — enjoys the “collaboration” element.

---

## Experiment 1 — AI ‘Micro-Plan’ Cards in Search Results

**Idea:**  
When a user searches for a city, instead of just showing “Top Spots” or “Start Planning,” also show 2–3 AI-generated micro-itinerary cards (e.g., “Perfect 1-day foodie tour”).

**Allocation & Conditions** *(user-level randomization)*:
- **Control:** Current search results → list/grid of points of interest.
- **T1:** Search results include 2 AI-generated micro-itinerary cards at top.
- **T2:** Same as T1 + cards show estimated cost/time + quick “Add to Trip” button.

**Hypothesis:**  
More users will start trips and add more places when inspiration is immediate.

**Leading Metrics:**
- % of searches resulting in a trip creation
- Average POIs added per session
- Add-to-trip rate from search

**Lagging Metrics:**
- Trip completion rate (users finalize itinerary)
- Repeat trip planning within 30 days
- Premium subscription conversion

---

## Experiment 2 — “Live Companion” On-Trip Mode

**Idea:**  
Once trip dates start, activate a “Live Companion” mode in mobile app that updates recommendations hourly based on location, weather, and current open hours.

**Allocation & Conditions** *(trip-level randomization)*:
- **Control:** Standard experience during trip.
- **T1:** Live Companion suggests 3–5 nearby recommendations daily via push.
- **T2:** Same as T1 but integrates “magic camera” suggestion prompts (e.g., snap a place and get a next stop).

**Hypothesis:**  
Context-aware, real-time suggestions will increase in-trip engagement and trust.

**Leading Metrics:**
- Daily active users during trip dates
- Click-through rate on Live Companion prompts
- % of suggested spots visited (based on GPS or manual confirmation)

**Lagging Metrics:**
- NPS post-trip
- Likelihood to use Mindtrip for next trip
- Social shares of itinerary

---

## Experiment 3 — Social “Trip Boards”

**Idea:**  
Let users create public or semi-private “Trip Boards” — curated lists of destinations/activities (like Pinterest boards for travel). Friends can follow, like, or clone them into their own plans.

**Allocation & Conditions** *(account-level randomization)*:
- **Control:** Current private-only itineraries.
- **T1:** Trip Boards with follow/like features enabled.
- **T2:** Trip Boards + AI-suggested items to enrich boards automatically over time.

**Hypothesis:**  
Social proof and public boards will drive inspiration, discovery, and new user acquisition.

**Leading Metrics:**
- Trip Board creation rate
- # of followers/likes per board
- % of cloned boards into new trips

**Lagging Metrics:**
- D30 retention for creators and followers
- Viral coefficient (invites sent / new users acquired)
- Premium conversion (if boards are a premium perk)
