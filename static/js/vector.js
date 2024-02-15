
ireland_lat_min = 51.296276
ireland_lat_max = 55.413426
ireland_lon_min = -10.684204
ireland_lon_max = -5.361328
cellSize = 0.1
latitude_width = (ireland_lat_max - ireland_lat_min)
longitude_height = (ireland_lon_max - ireland_lon_min)
num_rows = Math.floor(latitude_width/ cellSize)
num_cols = Math.floor(longitude_height/ cellSize)

class Particle{
    constructor(effect){
        this.effect = effect;
        this.x = Math.floor(Math.random()* this.effect.width);
        this.y = Math.floor(Math.random()* this.effect.height);
        this.speedX = Math.random()* 5 -2.5;
        this.speedY = Math.random()* 5 -2.5;
        this.history = [{x:this.x, y:this.y}]
        this.maxLength = 30;
        this.angle = 0;
    }
    draw(context){
        context.fillRect(this.x, this.y, 10, 10)
        context.beginPath();
        context.moveTo(this.history[0].x, this.history[0].y);
        for (let i = 0; i < this.history.length; i++){
            context.lineTo(this.history[i].x, this.history[i].y);
        }
        context.stroke();
    }
    update(){

        this.x += this.speedX;
        this.y += this.speedY;
        this.history.push({x: this.x, y: this.y});
        // If the length of the particle is greater than its max length property then shift array element
        if (this.history.length > this.maxLength){
            //shift array method removes one element from the beginning of an array
            this.history.shift();
        }
    }
}

class Effect {
    constructor(width,height){
        this.width = width;
        this.height = height;
        this.particles = [];
        this.numberOfParticles = 50;
        this.cellSize = 20;
        this.rows;
        this.cols;
        this.flowField = [];
        this.init();
    }
    init(){
        //create flow field
        this.rows = Math.floor(this.height/ this.cellSize);
        this.cols = Math.floor(this.width/ this.cellSize);
        this.flowField = [];
        for (let y=0; y < this.rows; y++){
            for (let x=0; x < this.cols; x++){
                let angle = Math.cos(x) + Math.sin(y);
                this.flowField.push(angle);
            }
        }
        // create particles
        for (let i = 0;i <this.numberOfParticles; i++){
        this.particles.push(new Particle(this))};
    }
    render(context){
        this.particles.forEach(particle=>{
            particle.draw(context);
            particle.update();
        })
    }
};

// Define Custom Canvas Layer Class
var CustomCanvasLayer = L.Layer.extend({
    onAdd: function(map) {
        this._map = map;
        this._canvas = L.DomUtil.create('canvas', 'leaflet-custom-canvas-layer');
        this._canvas.width =  map.getSize().x;
        this._canvas.height =  map.getSize().y;
        this._ctx = this._canvas.getContext('2d');
        this._ctx.fillStyle = 'white';
        this._ctx.strokeStyle = 'white';
        this._ctx.lineWidth = 1;
        map.getPanes().overlayPane.appendChild(this._canvas);
        // map.on('moveend', this._redraw, this);
        this._redraw();
    },

    onRemove: function(map) {
        map.getPanes().overlayPane.removeChild(this._canvas);
        map.off('moveend', this._redraw, this);
    },

    _redraw: function() {
        var bounds = this._map.getBounds();
        var topLeft = this._map.latLngToLayerPoint(bounds.getNorthWest());
        L.DomUtil.setPosition(this._canvas, topLeft);
        this.drawCanvas();
    },

    drawCanvas: function() {
        // Reference to the vector.js code
        const effect = new Effect(this._canvas.width, this._canvas.height);
        effect.render(this._ctx);
        animate(this._ctx,effect,this._canvas);
    }
});

// Animation loop
function animate(ctx,effect,canvas){
    ctx.clearRect(0,0,canvas.width, canvas.height);
    effect.render(ctx);
    //Pass anonymous function into requestAnimationFrame to pass parameters
    requestAnimationFrame(function(){animate(ctx,effect,canvas)});
}
