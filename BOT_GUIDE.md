# Space Mambot Command Guide

Welcome to the Space Mambot Command Guide. Here you can find details for every command that our fearless leader Space Mambot has to offer! Commands are grouped by feature and highlight the key rules and inputs players should know.

## Getting Started
- **/start** — Pick your starter deck, open the matching packs, and get your team role. The first thing all new users must do!

## Packs, Boxes, and Imports
- **/packlist** — Show the list of packs currently available for purchase.
- **/pack** `amount` — Buy/open packs from the available sets; `amount` optional default is 1, max 100.
- **/tin** — Buy a tin (promo plus five packs) from the available tin list. (Tins to be released later)
- **/box** — Buy/open a sealed box of 24 packs.
- **/quick_box** `pack_name` `amount` — Admin; open boxes quickly without per-pack animations; `amount` optional default is 1, max 10.
- **/cardpool_import** `ydk_file` `cardset` `cardrarity` `pack_csv` — Admin; import a YDK deck into a pack CSV with the given set name and rarity, reporting any missing IDs.

## Crafting & Fragmenting
- **/craft** `card` `amount` — Craft copies of a printing using shards; `amount` optional default is 1, max 3; starter cards and promos are un-craftable.
- **/fragment** `card` `amount` — Break down owned copies of a printing into shards; `amount` optional default is 1, max 100.
- **/fragment_bulk** `pack` `rarity` `keep` — Fragment owned cards from a pack at a specific rarity while keeping at least the chosen amount; `keep` optional default is 0; example: /fragment_bulk `Blazing Genesis` `Super Rare` `3` will fragment all your excess super rare cards owned from blazing genesis that you own more than 3 copies of
- **/shard_exchange** `from_shard` `to_shard` `amount` — Swap shards from one set to another using the current shard exchange rate.

## Wallets & Currency
- **/wallet** `user` — View mambucks and shard balances for yourself or another member; `user` optional default is you.

## Collection, Binders, Wishlists, and Trading
- **/collection** `set_number` — View your collection; `set_number` optional to filter by set.
- **/export_collection** — Get a CSV export of your collection compatible with ygoprodeck.
- **/wishlist_add** `card` `copies` — Add desired copies of a printing to your wishlist; `copies` optional default is 1.
- **/wishlist_remove** `card` `copies` — Remove copies from your wishlist entry; `copies` optional default is all copies in that entry.
- **/wishlist_display** `user` — Post the wishlist for you or another player; `user` optional default is you.
- **/wishlist_clear** — Clear your entire wishlist.
- **/binder_add** `card` `copies` — Move owned copies into your binder; `copies` optional default is 1, capped by how many you own.
- **/binder_remove** `card` `copies` — Remove copies from your binder; `copies` optional default is 1.
- **/binder_display** `user` — Show a player’s binder contents in the channel; `user` optional default is you.
- **/binder_clear** — Empty your binder completely.
- **/search** `card` — See who has a printing in binders and who wants it on wishlists.
- **/trade_start** `user` `...items...` — Offer up to five cards and optional shards to start a trade with another user; you must own what you offer.
- **/trade_accept** `trade_id` `...items...` — As the receiver, add your items to a pending trade; you must own what you offer.
- **/trade_show** `trade_id` — View the details of a specific trade.
- **/trade_confirm** `trade_id` — Confirm a trade you’re in; both sides must still own their items.
- **/trade_cancel** `trade_id` — Cancel a pending trade by ID or your most recent pending trade; `trade_id` optional defaults to your latest pending trade; only participants can cancel while it’s pending.

## Daily Rewards & Quests
- **/quests** — View your active quests and progress. Any completed quests will offer button to claim reward(s).
- **/quest_claim** `quest` — Claim a completed quest reward by name or ID. **<- WONT USE THIS IN PRACTICE WILL USE THE CLAIM BUTTONS FROM /QUESTS**
- **/quest_reset** `user` — Admin; reset all quest progress for a user.
- **/daily_mambucks** `amount` — Admin; view or update the daily mambuck grant for members; `amount` optional default is to show the current grant.
- **/daily_mambucks_total** — Admin; show the running total of maximum mambucks earnable per user.
- **/daily_mambucks_reset_total** — Admin; reset the tracked total of maximum mambucks earnable per user.

## Stats & Teams
- **/loss** `opponent` — Record a loss to another player; cannot target yourself or bots.
- **/stats** `user` — View win/loss record, win percentage, and team points for a member; `user` optional default is you.
- **/h2h** `opponent` — View head-to-head record between you and another member; no bots or self-targeting.
- **/team_award** `member` `points` — Admin; award team points to a member based on their team role.

## Gamba & Chips
- **/gamba** — Spend one gamba chip to spin Snipe Hunter’s slots for prizes; requires at least one chip. Users are automatically awarded 1 gamba chip per day.
- **/gamba_grant** `user` `amount` — Admin; grant gamba chips to a user; `amount` optional default is 1.

## Shop & Sales Utilities
- **/sales_reset** — Admin; reroll shop sale items and refresh the banner for the day.
- **/sales_show** — Admin; debug view of today’s sale rows.
- **/shop_refresh** — Admin; manually refresh the pinned shop message/banner in the shop channel.

## Tournaments
- **/tournament_create** `name` `format` `url_slug` — Create a Challonge tournament under the org; `url_slug` optional default is auto-generated; requires Manage Server.
- **/tournament_join** — Start a DM flow to submit your deck to a pending tournament; only works if a tournament is joinable.
- **/tournament_view** — List active tournaments with bracket links.
- **/tournament_standings** — Pick an active tournament to view standings.
- **/tournament_loss** `opponent` — Report a tournament match loss to another member; requires an active match between you and them.
- **/tournament_admin_loss** `loser` `winner` — Admin; report a result between two players for tournament matches.
- **/tournament_revert_result** — Admin; roll back the most recent recorded tournament result for a match.
- **/tournament_add_participant** `tournament` `member` — Admin; manually add a Discord member to a Challonge tournament.
- **/tournament_shuffle_seeds** `tournament` — Admin; shuffle participant seeds before a tournament starts.
- **/tournament_drop** `tournament` `member` — Admin; drop a participant from a tournament. <- **DONT USE, MUST DROP MANUALLY**
- **/deck_check** `deck_file` `tournament` — Validate a YDK deck against your collection and the banlist; `tournament` optional defaults to the currently relevant event if one exists.

## Gacha & Miscellaneous Fun
- **/pack**, **/tin**, **/box** — See Packs, Boxes, and Imports for opening content; some flows also tick quests.
- **/boop** — Send a playful boop response (image when available).
- **/gamba** — See Gamba & Chips for the slots minigame.

## System & Admin Utilities
- **/ping** — Quick bot liveness check (ephemeral).
- **/reload_data** — Admin; reload packs, tins, and shop data from disk.
- **/wallet_add** `user` `amount` `currency` — Admin; credit a wallet; `currency` optional defaults to mambucks.
- **/wallet_remove** `user` `amount` `currency` — Admin; debit a wallet; `currency` optional defaults to mambucks.
- **/admin_add_card** `user` `card` `qty` — Admin; add a specific printing to a user’s collection; `qty` optional default is 1; uses pack data for set info.
- **/admin_remove_card** `user` `card` `qty` — Admin; remove a specific printing from a user’s collection; `qty` optional default is 1.
- **/admin_reset_user** `user` `reason` — Admin; full account reset (collection, wallet, shards, quests, stats, wheel tokens, team roles); `reason` optional default is not specified.
- **/admin_report_loss** `loser` `winner` — Admin; manually record a match result for stats; both players must be non-bots.
- **/admin_revert_result** `loser` `winner` — Admin; roll back the most recent result between two players (undoes stats and quest ticks).
- **/admin_simulate_next_day** — Admin; simulate the next ET midnight rollover for daily rewards, gamba chips, sales, and quest rollovers.
- **/admin_reset_simulated_day** — Admin; clear simulated future-day markers so the next real rollover runs normally.
- **/admin_fragment_override_set** `card` `yield` `reason` — Admin; temporarily override shard yield for a printing; `reason` optional default is not specified.
- **/admin_fragment_override_clear** `target` — Admin; clear shard yield overrides by printing or by card plus set.
- **/admin_fragment_override_list** — Admin; list active shard-yield overrides.