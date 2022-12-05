DO $$
 DECLARE
     platform_id   platform.plat_id%TYPE;
     platform_name platform.plat_name%TYPE;

 BEGIN
     platform_id := 'ID';
     platform_name := 'PlatformName';
     FOR counter IN 1..20
         LOOP
            INSERT INTO Platform (plat_id, plat_name)
             VALUES (platform_id || counter, platform_name || counter);
         END LOOP;
 END;
 $$