# Verificar si los clusters son los adecuados.

cluster_model.fit = h2o.predict(object = model[1],  newdata = valid);
cluster_model.fit = as.data.frame(cluster_model.fit);

I = 45; # Cluster
chars_km.fit2 = which((cluster_model.fit == I) == 1);

nearest_center <- as.data.frame(xs[pos[I], 1:2])
nearest_center$concat <- paste(nearest_center$Pluid, nearest_center$dia, sep = "-");
patron <- dat2[dat2$concat == nearest_center$concat, 3:4];

Hpi1 <- Hpi(x=patron);
fhat.pi1 <- kde(x=patron, H=Hpi1);
u <- fhat.pi1$eval.points[[1]];
v <- fhat.pi1$eval.points[[2]];
est_dist2 <- fhat.pi1$estimate/sum(fhat.pi1$estimate)
p1 <- plot_ly(x = v, y = u, z= est_dist2, scene='scene1') %>% add_surface(showscale=FALSE)

i <- 15;
muestra <- as.data.frame(valid[chars_km.fit2[i], 1:2]);
muestra$concat <- paste(muestra$Pluid, muestra$dia, sep = "-");
dato <- dat2[dat2$concat == muestra$concat, 3:4];

Hpi1 <- Hpi(x=dato);
fhat.pi1 <- kde(x=dato, H=Hpi1);
u <- fhat.pi1$eval.points[[1]];
v <- fhat.pi1$eval.points[[2]];
est_dist <- fhat.pi1$estimate/sum(fhat.pi1$estimate)
p2 <- plot_ly(x = v, y = u, z= est_dist, scene='scene2') %>% add_surface(showscale=FALSE) 

i <- 6;
muestra <- as.data.frame(valid[chars_km.fit2[i], 1:2]);
muestra$concat <- paste(muestra$Pluid, muestra$dia, sep = "-");
dato <- dat2[dat2$concat == muestra$concat, 3:4];

Hpi1 <- Hpi(x=dato);
fhat.pi1 <- kde(x=dato, H=Hpi1);
u <- fhat.pi1$eval.points[[1]];
v <- fhat.pi1$eval.points[[2]];
est_dist <- fhat.pi1$estimate/sum(fhat.pi1$estimate)
p3 <- plot_ly(x = v, y = u, z= est_dist, scene='scene3') %>% add_surface(showscale=FALSE) 

i <- 2;
muestra <- as.data.frame(valid[chars_km.fit2[i], 1:2]);
muestra$concat <- paste(muestra$Pluid, muestra$dia, sep = "-");
dato <- dat2[dat2$concat == muestra$concat, 3:4];

Hpi1 <- Hpi(x=dato);
fhat.pi1 <- kde(x=dato, H=Hpi1);
u <- fhat.pi1$eval.points[[1]];
v <- fhat.pi1$eval.points[[2]];
est_dist <- fhat.pi1$estimate/sum(fhat.pi1$estimate)
p4 <- plot_ly(x = v, y = u, z= est_dist, scene='scene4') %>% add_surface(showscale=FALSE) 

subplot(p1, p2, p3, p4)  %>%
  layout(title = "3D Subplots",
         scene1 = list(domain=list(x=c(0,0.5),y=c(0.5,1)),
                       aspectmode='cube'),
         scene2 = list(domain=list(x=c(0.5,1),y=c(0.5,1)),
                       aspectmode='cube'),
         scene3 = list(domain=list(x=c(0,0.5),y=c(0,0.5)),
                       aspectmode='cube'),
         scene4 = list(domain=list(x=c(0.5,1),y=c(0,0.5)),
                       aspectmode='cube'))

return(cluster_model)
