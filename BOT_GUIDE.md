# Space Mambot Command Guide

Welcome to the Space Mambot Command Guide. Here you can find details for every command that our fearless leader Space Mambot has to offer! Commands are grouped by feature and highlight the key rules and inputs players should know.

## Getting Started
- **/start** — Pick your starter deck, open the matching packs, and get your team role. The first thing all new users must do!
- **/join_team** `team` — Join the active team for the current set (with auto pack reward) if you do not already have a team role. (Will be used for sets after set 1)

## Obtaining Packs (Check Shop Channel For Prices)
- **/pack** `amount` — Buy/open packs from the available sets; `amount` optional default is 1, max 100.
- **/tin** — Buy a tin (promo plus five packs) from the available tin list. (Tins to be released later)
- **/box** — Buy/open a sealed box of 24 packs.

## Checking Your Inventory
- **/wallet** `user` — View mambucks and shard balances for yourself or another member; `user` optional default is you.
- **/collection** `set_number` `include_binder` — View your collection; `set_number` optional to filter by set; `include_binder` optional default true, set to false to
exclude cards in your binder from collection output
- **/export_collection** — Get a CSV export of your collection compatible with ygoprodeck. NOTE: For imports into ygopro, you must delete your collection each time prior
to importing
- **/card** `cardname` — View card details for a given cardname

## Crafting & Fragmenting
- **/craft** `card` `amount` — Craft individual cards using shards; `amount` optional default is 1, max 3; promo cards are un-craftable.
- **/fragment** `card` `amount` — Break down owned copies of cards into shards; `amount` optional default is 1, max 100.
- **/fragment_bulk** `pack` `rarity` `keep` — Fragment owned cards from a pack at a specific rarity while keeping at least the chosen amount; `keep` optional default is 3; example: /fragment_bulk `Blazing Genesis` `Super Rare` `3` will fragment all your excess super rare cards owned from blazing genesis that you own more than 3 copies of

## Trading, Binders, and Wishlists
- **/trade_start** `user` `...items...` — Offer up to five cards and/or shards to start a trade with another user; At least 1 card must be involved with any trade
- **/trade_accept** `trade_id` `...items...` — As the receiver, add your items to a pending trade.
- **/trade_cancel** `trade_id` — Cancel a pending trade by ID or your most recent pending trade; `trade_id` optional defaults to your latest pending trade.

## Binders and Wishlists
- **/search** `card` — See who has a given card in binders and who wants it on wishlists.
- **/binder** `user` — Show a player’s binder contents in the channel; `user` optional default is you.
- **/binder_add** `card` `copies` — Move owned cards into your binder; `copies` optional default is 1, capped by how many you own.
- **/binder_bulk_add** `min_rarity` `exact_rarity` `pack` `excess_amount` — Add extra copies from your collection into your binder; `min_rarity` optional default is common, `exact_rarity` optional to match a specific rarity, `pack` optional to target a specific set, `excess_amount` optional default is 3 copies to keep.
- **/binder_remove** `card` `copies` — Remove cards from your binder; `copies` optional default is 1.
- **/binder_clear** — Empty your binder completely.
- **/wishlist** `user` — Post the wishlist for you or another player; `user` optional default is you.
- **/wishlist_add** `card` `copies` — Add desired cards to your wishlist; `copies` optional default is 1.
- **/wishlist_remove** `card` `copies` — Remove cards from your wishlist; `copies` optional default is all copies in your wishlist currently.
- **/wishlist_clear** — Clear your entire wishlist.

## Daily Rewards
- **/daily** — View your daily duel progress and claim available rewards.
- **/gamba** — Spend one gamba chip to spin Snipe Hunter’s slots for prizes. Users are automatically awarded 1 gamba chip per day.

## Duels and Stats
- **/deck_check** `deck_file` — Validate a YDK deck against your collection and the banlist
- **/join_queue** `duelingbook_name` — Join the rated duel queue in **#duel-arena** (must be used in that channel). The Duelingbook name is required the first time you
join queue and may be provided if you wish to update your duelingbook name
- **/leave_queue** — Leave the rated duel queue if you are waiting for a pairing.
- **/report** `opponent`, `outcome` — Report the result of a queued duel against your opponent. Either the winner or loser of a match may use this.
- **/stats** `user` — View win/loss record, win percentage, and team points for a member; `user` optional default is you.
- **/h2h** `opponent` — View head-to-head record between you and another member; no bots or self-targeting.

## Tournaments
- **/tournament_join** — Start a DM flow to submit your deck to a pending tournament; only works if a tournament is joinable.
- **/tournament_standings** — Pick an active tournament to view standings.
- **/tournament_report** `winner` `loser` `replay_url` — Report a tournament match result; either participant can use it; `replay_url` required when the tournament enforces replays.
- **/tournament_drop** `tournament` `member` — Drop from a tournament. If the tournament is active, you will need an admin to manually update in challonge.
- **/timer** — Show the remaining time on the active round timer (if any)

## Miscellaneous
- **/boop** — Give the bot a little boop on the snoot and see what he has to say!
- **/ping** — Quick bot liveness check.

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Admin Commands
Admin-only commands for the bot.

## Packs, Boxes, and Imports
- **/quick_box** `pack_name` `amount` — Open boxes or bundles quickly without per-pack animations; `amount` default is 1, max 10.
- **/cardpool_import** `ydk_file` `cardset` `cardrarity` `pack_csv` — Import a YDK deck into a pack CSV with the given set name and rarity, reporting any missing IDs.
- **/admin_award_pack** `user` `pack_name` `amount` — Open packs for a user and DM them the pulls.
- **/admin_award_box** `user` `pack_name` `amount` — Open a sealed box (up to 5) for a user and DM them the pulls.

## Collection & Wallet Management
- **/wallet_add** `user/all_users` `amount` `currency` `shard_set` — Credit a wallet (or all starter-role members) with mambucks or shards.
- **/wallet_remove** `user/all_users` `amount` `currency` `shard_set` — Debit a wallet (or all starter-role members) with mambucks or shards.
- **/admin_add_card** `user` `card` `qty` — Add a specific printing to a user’s collection; `qty` optional default is 1; uses pack data for set info.
- **/admin_remove_card** `user` `card` `qty` — Remove a specific printing from a user’s collection; `qty` optional default is 1.
- **/admin_reset_user** `user` `reason` — Full account reset (collection, wallet, shards, quests, stats, wheel tokens, team roles).

## Quests & Daily Rewards
- **/admin_reset_user_quests** `user` `reason` — Clear all quest progress/claims for a user without touching other data.
- **/admin_daily_duel_status** `user` — Inspect daily duel rollover slots and queued claimables for a user.
- **/daily_mambucks** `amount` — View or update the daily mambuck grant for members; `amount` optional default is to show the current grant.
- **/daily_mambucks_total** — Show the running total of maximum mambucks earnable per user.
- **/daily_mambucks_reset_total** — Reset the tracked total of maximum mambucks earnable per user.
- **/daily_mambucks_set_total** `total` — Manually set the running total of daily mambucks earnable per user.
- **/daily_packs_total** `quest_id` — View the running total of daily quest packs earnable per user (defaults to the week 1 daily duel quest).
- **/daily_packs_reset_total** `quest_id` — Reset the tracked total of daily quest packs earnable per user (defaults to the week 1 daily duel quest).

## Stats & Teams
- **/admin_report_loss** `loser` `winner` — Manually record a match result for stats; both players must be non-bots.
- **/admin_revert_result** `loser` `winner` — Roll back the most recent result between two players (undoes stats and quest ticks).
- **/admin_cancel_match** `player_a` `player_b` — Cancel an active duel pairing so the players can requeue.
- **/team_award** `member` `points` — Award team points to a member based on their team role.
- **/team_split_points** — Split duel team points based on recorded wins for the active set.
- **/team_reset_points** `set_id` `member` — Clear team point splits for a set (optionally for a single member).

## Gamba & Chips
- **/gamba_grant** `user` `amount` — Grant gamba chips to a user; `amount` optional default is 1.

## Sales & Shop Utilities
- **/sales_reset** — Reroll shop sale items and refresh the banner for the day.
- **/sales_show** — Debug view of today’s sale rows.
- **/shop_refresh** — Manually refresh the pinned shop message/banner in the shop channel.

## Tournament Management
- **/tournament_create** `name` `format` `url_slug` `replays_required` — Create a Challonge tournament under the org; `url_slug` optional default is auto-generated; set `replays_required` to require replay links on match reports; requires Manage Server.
- **/tournament_admin_loss** `loser` `winner` `replay_url` — Report a result between two players for tournament matches; accepts `replay_url` when replays are enforced.
- **/tournament_revert_result** — Roll back the most recent recorded tournament result for a match.
- **/tournament_add_participant** `tournament` `member` — Manually add a Discord member to a Challonge tournament.
- **/tournament_shuffle_seeds** `tournament` — Shuffle participant seeds before a tournament starts.
- **/set_timer** `minutes` — Set a server-wide timer duration in minutes.

## System Maintenance
- **/reload_data** — Reload packs, tins, and shop data from disk.
- **/admin_simulate_next_day** — Simulate the next ET midnight rollover for daily rewards, gamba chips, sales, and quest rollovers.
- **/admin_reset_simulated_day** — Clear simulated future-day markers so the next real rollover runs normally.
- **/admin_fragment_override_set** `card` `yield` `reason` — Temporarily override shard yield for a printing; `reason` optional default is not specified.
- **/admin_fragment_override_clear** `target` — Clear shard yield overrides by printing or by card plus set.
- **/admin_fragment_override_list** — List active shard-yield overrides.
- **/admin_end_set** `mambuck_to_shards` — Convert all mambuck balances into shards for the active set at the given ratio and clear all queued daily quest entries.