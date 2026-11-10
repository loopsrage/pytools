"""Live Gmail auto-sort test. Opens a browser for consent, scans full inbox history
page by page: scan a page, create labels and archive matches, then next page."""

from gmaillib.gmail import GmailClient, GmailSorter


def main():
    client = GmailClient()
    print("Authenticated.")

    all_ids = client.all_message_ids(query="in:inbox")
    total = len(all_ids)
    print(f"Total inbox messages: {total}")

    sorter = GmailSorter(client)
    scanned = [0]

    def progress(current, total_count, sender):
        scanned[0] = current
        if current % 25 == 0 or current == total_count:
            pct = current / total_count * 100 if total_count else 0
            print(f"  Scanned {current}/{total_count} ({pct:.1f}%): {sender[:60]}")

    # Process page by page: scan page -> apply labels -> next page
    categories = sorter.auto_categorize_inbox(
        threshold=50,
        min_group_size=2,
        page_size=500,
        on_progress=progress,
    )

    print(f"\n=== Auto-categories applied ({len(categories)} categories) ===")
    for name, matches in sorted(categories.items(), key=lambda x: -len(x[1])):
        print(f"\n{name} ({len(matches)} messages):")
        for m in matches[:10]:
            print(f"  [{m['score']}] {m['from']} - {m['subject']}")

    print("\nDone. Labels created and matching messages archived out of inbox.")


if __name__ == "__main__":
    main()
