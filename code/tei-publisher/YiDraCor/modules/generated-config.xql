xquery version "3.1";

(:~
 : Generated configuration – do not edit
 :)
module namespace config="https://e-editiones.org/tei-publisher/generator/config";

declare variable $config:webcomponents := "2.26.0-next-3.16";
declare variable $config:webcomponents-cdn := "https://cdn.jsdelivr.net/npm/@teipublisher/pb-components";
declare variable $config:fore := "";

declare variable $config:default-view := "div";
declare variable $config:default-template := "basic.html";
declare variable $config:default-media := ("web", "print", "fo", "latex", "epub");
declare variable $config:search-default := "";
declare variable $config:sort-default := "category";


    
    declare variable $config:data-root := $config:app-root || "/data";
    



    
    declare variable $config:data-default := $config:data-root || "/annotate";
    


declare variable $config:odd-root := $config:app-root || "/resources/odd";
declare variable $config:default-odd := "teipublisher.odd";
declare variable $config:odd-internal := 
    (  );

declare variable $config:odd-available :=

( "teipublisher.odd", "annotations.odd" )
;


(:
    Determine the application root collection from the current module load path.
:)
declare variable $config:app-root :=
    let $rawPath := system:get-module-load-path()
    let $modulePath :=
        (: strip the xmldb: part :)
        if (starts-with($rawPath, "xmldb:exist://")) then
            if (starts-with($rawPath, "xmldb:exist://embedded-eXist-server")) then
                substring($rawPath, 36)
            else
                substring($rawPath, 15)
        else
            $rawPath
    return
        substring-before($modulePath, "/modules")
;

declare variable $config:pagination-depth := 10;

declare variable $config:pagination-fill := 5;

declare variable $config:address-by-id as xs:boolean :=  false() ;

declare variable $config:default-language as xs:string := "";

(:~
 : Use the JSON configuration to determine which configuration applies for which collection
 :)
declare function config:collection-config($collection as xs:string?, $docUri as xs:string?) {
    
    (: No special overrides apply. Return the default in all cases:)
        ()
    
};