xquery version "3.1";

declare namespace api="https://tei-publisher.com/xquery/api";
declare namespace output="http://www.w3.org/2010/xslt-xquery-serialization";
declare namespace tei="http://www.tei-c.org/ns/1.0";

import module namespace roaster="http://e-editiones.org/roaster";
import module namespace auth="http://e-editiones.org/roaster/auth";
import module namespace rutil="http://e-editiones.org/roaster/util";
import module namespace dapi="http://teipublisher.com/api/documents" at "api/document.xql";
import module namespace capi="http://teipublisher.com/api/collection" at "api/collection.xql";
import module namespace sapi="http://teipublisher.com/api/search" at "api/search.xql";
import module namespace dts="http://teipublisher.com/api/dts" at "api/dts.xql";
import module namespace iapi="http://teipublisher.com/api/info" at "api/info.xql";
import module namespace vapi="http://teipublisher.com/api/view" at "api/view.xql";
import module namespace iiif="https://e-editiones.org/api/iiif" at "api/iiif.xql";
import module namespace nlp="http://teipublisher.com/api/nlp" at "api/nlp.xql";
import module namespace rapi="http://teipublisher.com/api/registers" at "../registers.xql";
import module namespace custom="http://teipublisher.com/api/custom" at "../custom-api.xql";
import module namespace action="http://teipublisher.com/api/actions" at "api/actions.xql";
import module namespace deploy="https://teipublisher.org/api/deploy" at "api/deploy.xql";


import module namespace anno="http://teipublisher.com/api/annotations" at "../annotations/annotations.xql";


declare option output:indent "no";

let $lookup := function($name as xs:string) {
    try {
        let $cfun := custom:lookup($name, 1)
        return
            if (empty($cfun)) then
                function-lookup(xs:QName($name), 1)
            else
                $cfun
    } catch * {
        ()
    }
}
let $resp := roaster:route(
    (
        
        
        "modules/annotations/api.json",
        
        
        "modules/custom-api.json",
        "modules/lib/api.json"
    ), $lookup)
return
    $resp