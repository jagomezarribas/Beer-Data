var width = $(window).width(); 
window.onscroll = function(){
if ((width >= 1000)){
    if(document.body.scrollTop > 80 || document.documentElement.scrollTop > 80) {
        $("#header").css("background","#fff");    //antes estaba #fff
        $("#header").css("color","#000");           //antes estaba #000
        $("#header").css("box-shadow","0px 0px 20px rgba(0,0,0,0.09)");
        $("#header").css("padding","4vh 4vw");
        //$("#header").css("width","100%");
        $("#navigation a").hover(function(){
            $(this).css("border-bottom","2px solid rgb(226, 198, 35)");
        },function(){
            $(this).css("border-bottom","2px solid transparent");
        });
    }else{
        $("#header").css("background","fff");   //antes estaba "transparent"
        $("#header").css("color","#000");
        $("#header").css("box-shadow","0px 0px 0px rgba(0, 0, 0,0.09);");    //rgba(0,0,0,0) es negro
        $("#header").css("padding","6vh 4vw");
        //$("#header").css("width","100%");
        $("#navigation a").hover(function(){
            $(this).css("border-bottom","2px solid #fff");
        },function(){
            $(this).css("border-bottom","2px solid transparent");
        });
    }
}
}

function magnify(imglink){
    $("#img_here").css("background",`url('${imglink}') center center`);
    $("#magnify").css("display","flex");
    $("#magnify").addClass("animated fadeIn");
    setTimeout(function(){
        $("#magnify").removeClass("animated fadeIn");
    },800);
}

function closemagnify(){
    $("#magnify").addClass("animated fadeOut");
    setTimeout(function(){
        $("#magnify").css("display","none");
        $("#magnify").removeClass("animated fadeOut");
        $("#img_here").css("background",`url('') center center`);
    },800);
}

setTimeout(function(){
    $("#loading").addClass("animated fadeOut");
    setTimeout(function(){
      $("#loading").removeClass("animated fadeOut");
      $("#loading").css("display","none");
    },800);
},1650);

$(document).ready(function(){
    $("a").on('click', function(event) {
      if (this.hash !== "") {
        event.preventDefault();
        var hash = this.hash;
        var headerHeight = $("#header").height(); //linea añadida (antes no estaba)
        $('body,html').animate({
        scrollTop: $(hash).offset().top - headerHeight  //añadimos -headerH
        }, 1800, function(){ 
        window.location.hash = hash;
       });
       } 
      });
  });
  