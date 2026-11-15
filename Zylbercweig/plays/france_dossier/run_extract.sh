#!/bin/zsh
cd "$(dirname "$0")"
EXTRA='This run builds a dossier on FRANCE (Paris and every other French place). In addition to the standard fields, add to EVERY station a field "country": the modern country of the place in English ("France", "Poland", "USA", ...; "" if unknown). Be exhaustive about stations in France: Paris (and its venues, quarters, suburbs), Metz, Strasbourg, Nancy, Lyon, Marseille, Nice, Alsace-Lorraine, wartime internment camps (Drancy, Gurs, Pithiviers, Beaune-la-Rolande, Rivesaltes, Le Vernet), hiding in the south, deportation from France. For French stations, put Paris venues/theatres/troupes/clubs in "org"/"venue" verbatim and list every named play performed there as an event. Third-person testimony about other people in France counts. The adjective "French" (language, translation from French) is NOT a station.'
for s in shard_0?; do
  python3.11 ../itinerary/extract_itineraries.py --ids-file $s --out drafts_${s}.jsonl --prompt-extra "$EXTRA" > log_${s}.txt 2>&1 &
done
wait
