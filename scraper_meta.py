            for card in soup.select("article, li, .card, .ListingCard, [data-testid*='listing-card']"):
                a = card.find("a", href=True)
                if not a or "/detail/" not in a.get("href", ""):
                    continue
                abs_url = urljoin(page_url, a["href"])
                texts = []
                for sel in ["h2", "h3", "[class*='title']", "[data-testid*='title']"]:
                    el = card.select_one(sel)
                    if el:
                        t = clean_spaces(el.get_text(" "))
                        if t and len(t) <= 160:
                            texts.append(t)
                for badge in card.select(".badge, .chip, .tag, [class*='badge'], [class*='chip'], [class*='tag']"):
                    t = clean_spaces(badge.get_text(" "))
                    if t and len(t) <= 60:
                        texts.append(t)
                for small in card.select("small, .subtitle, [class*='subtitle']"):
                    t = clean_spaces(small.get_text(" "))
                    if t and len(t) <= 100:
                        texts.append(t)
                hint = best_type_match(texts)
                if hint:
                    mapping[abs_url] = hint
            list_type_hints.update(mapping)
        except Exception as e:
            if VERBOSE:
                print(f"[WARN] type-hints parse error on page {page}: {e}", flush=True)

        # parsovanie detailov
        for idx, url in enumerate(links, start=1):
            if max_listings_total is not None and total_added >= max_listings_total:
                break

            type_hint = list_type_hints.get(url)
            try:
                rec = parse_detail(s, url, seq_in_run=idx, type_hint=type_hint)
            except Exception as e:
                if VERBOSE:
                    print(f"[ERROR] detail fail {url} -> {e}", flush=True)
                continue

            pk = rec.get("pk") or url
            existing_idx = master.index[master["pk"] == pk].tolist()

            if existing_idx:
                # update existujúceho záznamu
                i = existing_idx[0]
                row = master.loc[i].to_dict()

                today_state = {"price_eur": rec.get("price_eur"), "price_note": rec.get("price_note")}
                row = append_history_if_changed(row, today_state)

                for k in [
                    "url", "listing_id",
                    "obec", "street_or_locality",
                    "title", "property_type", "rooms", "area_m2", "condition",
                    "description_text", "features_json",
                    "land_area_m2", "builtup_area_m2", "plot_width_m",
                    "orientation", "ownership", "terrain",
                    "water", "electricity", "gas", "waste", "heating", "rooms_count",
                ]:
                    v = rec.get(k)
                    if v is not None and v != "":
                        row[k] = v

                row["last_seen"] = TODAY
                row["active"] = True
                row["inactive_since"] = None
                row["scraped_at"] = rec.get("scraped_at")
                row["seen_today"] = True

                for k, v in row.items():
                    if k in master.columns:
                        master.at[i, k] = v

                # namiesto sťahovania obrázkov – uložíme URL do stagingu
                seq_global_val = int(master.at[i, "seq_global"]) if pd.notna(master.at[i, "seq_global"]) else None
                listing_id_val = rec.get("listing_id")
                rank = 1
                for uimg in rec.get("_image_urls") or []:
                    images_rows.append({
                        "pk": pk, "listing_id": listing_id_val, "seq_global": seq_global_val,
                        "url": rec.get("url"), "image_url": uimg, "image_rank": rank
                    })
                    rank += 1

            else:
                # nový záznam
                next_seq += 1
                rec_out = {c: None for c in master.columns}
                rec_out.update(rec)

                rec_out["seq_global"] = next_seq
                rec_out["first_seen"] = TODAY
                rec_out["last_seen"] = TODAY
                rec_out["active"] = True
                rec_out["inactive_since"] = None
                rec_out["price_status"] = current_price_status(rec.get("price_eur"), rec.get("price_note"))
                rec_out["price_changes_count"] = 0
                rec_out["price_history"] = "[]"
                rec_out["seen_today"] = True

                rec_out = append_history_if_changed(
                    rec_out, {"price_eur": rec.get("price_eur"), "price_note": rec.get("price_note")}
                )

                master = pd.concat([master, pd.DataFrame([rec_out])], ignore_index=True)

                # staging URL obrázkov pre nový záznam
                rank = 1
                for uimg in rec.get("_image_urls") or []:
                    images_rows.append({
                        "pk": rec_out["pk"], "listing_id": rec.get("listing_id"), "seq_global": next_seq,
                        "url": rec.get("url"), "image_url": uimg, "image_rank": rank
                    })
                    rank += 1

            total_added += 1
            if PROGRESS_EVERY and (total_added % PROGRESS_EVERY == 0):
                print(f"[{total_added}] {url}", flush=True)

            sleep_a_bit()

    # označ nevidené ako neaktívne
    if "seen_today" in master.columns:
        mask_unseen = master["seen_today"] != True
        master.loc[mask_unseen & (master["active"] == True), "inactive_since"] = TODAY
        master.loc[mask_unseen, "active"] = False

    # prepočítaj days_listed
    master = finalize_days(master)

    # snapshot iba dnešné
    try:
        snap = master[master["seen_today"] == True].copy()
    except Exception:
        snap = master.copy()

    # persist XLSX (debug)
    if SAVE_EXCEL:
        save_excel(master.drop(columns=["seen_today"]), MASTER_XLSX)
        save_excel(snap.drop(columns=["seen_today"], errors="ignore"), SNAPSHOT_XLSX)
        print(f"✅ Uložený master:   {MASTER_XLSX}", flush=True)
        print(f"✅ Uložený snapshot: {SNAPSHOT_XLSX}", flush=True)

    # upload do BQ (v2)
    images_df = pd.DataFrame(images_rows, columns=["pk","listing_id","seq_global","url","image_url","image_rank"])
    upload_to_bigquery(master.drop(columns=["seen_today"], errors="ignore"), snap.drop(columns=["seen_today"], errors="ignore"), images_df)
